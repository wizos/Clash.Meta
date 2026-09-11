package outboundgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"

	S "github.com/metacubex/sing/common"
	SN "github.com/metacubex/sing/common/network"
)

const (
	failoverRaceLimit       = 64
	failoverRaceSlotLimit   = 4096
	failoverRaceSuppression = time.Second
)

type failoverRaceOwnerKey struct{}

func failoverRaceOwned(ctx context.Context) bool {
	owned, _ := ctx.Value(failoverRaceOwnerKey{}).(bool)
	return owned
}

var failoverRaceExtra = make(chan struct{}, failoverRaceLimit)

var failoverRaceSlots = struct {
	sync.Mutex
	entries map[string]failoverRaceSlot
}{entries: map[string]failoverRaceSlot{}}

type failoverRaceSlot struct {
	active bool
	until  time.Time
}

func acquireFailoverRaceSlot(key string) (func(), bool) {
	select {
	case failoverRaceExtra <- struct{}{}:
	default:
		return nil, false
	}

	now := time.Now()
	failoverRaceSlots.Lock()
	for entryKey, slot := range failoverRaceSlots.entries {
		if !slot.active && !now.Before(slot.until) {
			delete(failoverRaceSlots.entries, entryKey)
		}
	}
	if slot, found := failoverRaceSlots.entries[key]; found && (slot.active || now.Before(slot.until)) {
		failoverRaceSlots.Unlock()
		<-failoverRaceExtra
		return nil, false
	}
	if len(failoverRaceSlots.entries) >= failoverRaceSlotLimit {
		failoverRaceSlots.Unlock()
		<-failoverRaceExtra
		return nil, false
	}
	failoverRaceSlots.entries[key] = failoverRaceSlot{active: true}
	failoverRaceSlots.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			failoverRaceSlots.Lock()
			failoverRaceSlots.entries[key] = failoverRaceSlot{until: time.Now().Add(failoverRaceSuppression)}
			failoverRaceSlots.Unlock()
			<-failoverRaceExtra
		})
	}, true
}

func failoverRaceTarget(metadata *C.Metadata) string {
	return strings.ToLower(metadata.RemoteAddress())
}

func failoverRaceLeaf(proxy C.Proxy, metadata *C.Metadata) string {
	const maxDepth = 16
	for depth := 0; depth < maxDepth; depth++ {
		switch proxy.Type() {
		case C.URLTest, C.Fallback, C.Selector:
		default:
			return proxy.Name()
		}
		next := proxy.Unwrap(metadata, false)
		if next == nil || next.Name() == proxy.Name() {
			return proxy.Name()
		}
		proxy = next
	}
	return proxy.Name()
}

type failoverRaceSide uint8

const (
	failoverRaceNone failoverRaceSide = iota
	failoverRacePrimary
	failoverRaceChallenger
)

type failoverRacePath struct {
	proxy    C.Proxy
	conn     C.Conn
	dialDone bool
	failed   bool
	err      error
}

type failoverRaceDial func(context.Context, C.Proxy, *C.Metadata) (C.Conn, error)

type failoverRaceReadResult struct {
	side failoverRaceSide
	data []byte
	n    int
	err  error
}

type firstIOFailoverConn struct {
	C.Conn // The existing tracker contract intentionally keeps this initial path snapshot.

	mu               sync.Mutex
	primary          failoverRacePath
	challenger       failoverRacePath
	initial          failoverRaceSide
	winner           failoverRaceSide
	confirmed        bool
	closed           bool
	writeInProgress  bool
	writeDone        chan struct{}
	readDeadline     time.Time
	writeDeadline    time.Time
	readDeadlineSet  bool
	writeDeadlineSet bool
	deadlineMu       sync.Mutex

	notify            chan struct{}
	triggerChallenger chan struct{}
	primaryFailed     chan struct{}
	dialCtx           context.Context
	cancelDial        context.CancelFunc
	releaseChallenger func()
	releaseOnce       sync.Once
	onConfirmed       func()

	readMu  sync.Mutex
	writeMu sync.Mutex
}

func dialFirstIOFailover(
	ctx context.Context,
	metadata *C.Metadata,
	primary C.Proxy,
	challenger C.Proxy,
	delay time.Duration,
	dial failoverRaceDial,
	onConfirmed func(),
) (C.Conn, error) {
	if delay < 0 {
		delay = 0
	}
	dialCtx, cancelDial := context.WithCancel(context.WithValue(ctx, failoverRaceOwnerKey{}, true))
	raced := &firstIOFailoverConn{
		primary:           failoverRacePath{proxy: primary},
		challenger:        failoverRacePath{proxy: challenger},
		notify:            make(chan struct{}, 1),
		triggerChallenger: make(chan struct{}, 1),
		primaryFailed:     make(chan struct{}),
		dialCtx:           dialCtx,
		cancelDial:        cancelDial,
		onConfirmed:       onConfirmed,
	}

	go raced.dialPrimary(metadata.Clone(), dial)
	go raced.dialChallenger(metadata.Clone(), delay, dial, failoverRaceLeaf(challenger, metadata)+"\x00"+failoverRaceTarget(metadata))

	if err := raced.waitInitial(ctx); err != nil {
		_ = raced.Close()
		return nil, err
	}
	return raced, nil
}

func (c *firstIOFailoverConn) dialPrimary(metadata *C.Metadata, dial failoverRaceDial) {
	conn, err := dial(c.dialCtx, c.primary.proxy, metadata)
	c.finishDial(failoverRacePrimary, conn, err)
	if err != nil {
		close(c.primaryFailed)
	}
}

func (c *firstIOFailoverConn) dialChallenger(metadata *C.Metadata, delay time.Duration, dial failoverRaceDial, slotKey string) {
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-timer.C:
	case <-c.triggerChallenger:
	case <-c.primaryFailed:
	case <-c.dialCtx.Done():
		c.finishDial(failoverRaceChallenger, nil, c.dialCtx.Err())
		return
	}

	release, acquired := acquireFailoverRaceSlot(slotKey)
	if !acquired {
		c.finishDial(failoverRaceChallenger, nil, nil)
		return
	}
	c.mu.Lock()
	c.releaseChallenger = release
	c.mu.Unlock()

	conn, err := dial(c.dialCtx, c.challenger.proxy, metadata)
	discarded := c.finishDial(failoverRaceChallenger, conn, err)
	if err != nil || discarded {
		c.releaseExtra()
	}
}

func (c *firstIOFailoverConn) finishDial(side failoverRaceSide, conn C.Conn, err error) bool {
	c.deadlineMu.Lock()
	if conn != nil {
		c.applyStoredDeadlinesLocked(conn)
	}
	c.mu.Lock()
	path := c.pathLocked(side)
	path.dialDone = true
	path.err = err
	if err != nil {
		path.failed = true
	}
	closeConn := conn != nil && (c.closed || c.winner != failoverRaceNone && c.winner != side)
	if !closeConn {
		path.conn = conn
	}
	c.mu.Unlock()
	c.deadlineMu.Unlock()
	if closeConn {
		_ = conn.Close()
	}
	c.signal()
	return closeConn
}

func (c *firstIOFailoverConn) waitInitial(ctx context.Context) error {
	for {
		c.mu.Lock()
		if c.primary.conn != nil {
			c.initial = failoverRacePrimary
			c.Conn = c.primary.conn
			c.mu.Unlock()
			return nil
		}
		if c.challenger.conn != nil {
			c.initial = failoverRaceChallenger
			c.Conn = c.challenger.conn
			c.mu.Unlock()
			return nil
		}
		finished := c.primary.dialDone && c.challenger.dialDone
		err := c.combinedErrorLocked()
		c.mu.Unlock()
		if finished {
			return err
		}
		select {
		case <-c.notify:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *firstIOFailoverConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if conn := c.winnerConn(); conn != nil {
		return conn.Read(p)
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if conn := c.winnerConn(); conn != nil {
		return conn.Read(p)
	}

	results := make(chan failoverRaceReadResult, 2)
	started := map[failoverRaceSide]bool{}
	active := 0

	for {
		c.mu.Lock()
		winner := c.winner
		for _, side := range []failoverRaceSide{failoverRacePrimary, failoverRaceChallenger} {
			path := c.pathLocked(side)
			if path.conn == nil || path.failed || started[side] || winner != failoverRaceNone && winner != side {
				continue
			}
			started[side] = true
			active++
			conn := path.conn
			side := side
			go func() {
				data := make([]byte, len(p))
				n, err := conn.Read(data)
				results <- failoverRaceReadResult{side: side, data: data, n: n, err: err}
			}()
		}
		noPotential := c.noPotentialLocked()
		err := c.combinedErrorLocked()
		c.mu.Unlock()

		if noPotential && active == 0 {
			return 0, err
		}
		select {
		case result := <-results:
			active--
			if result.n > 0 {
				if c.selectReadWinner(result.side) {
					copy(p, result.data[:result.n])
					c.drainReads(results, active)
					return result.n, result.err
				}
				continue
			}
			if result.err == nil {
				result.err = io.ErrNoProgress
			}
			c.mu.Lock()
			winner := c.winner
			c.mu.Unlock()
			if winner == result.side {
				c.drainReads(results, active)
				return 0, result.err
			}
			c.failPath(result.side, result.err)
		case <-c.notify:
		}
	}
}

func (c *firstIOFailoverConn) selectReadWinner(side failoverRaceSide) bool {
	for {
		c.mu.Lock()
		if c.writeInProgress {
			done := c.writeDone
			c.mu.Unlock()
			<-done
			continue
		}
		if c.pathLocked(side).failed {
			c.mu.Unlock()
			return false
		}
		if c.winner != failoverRaceNone && c.winner != side {
			c.mu.Unlock()
			return false
		}
		newWinner := c.winner == failoverRaceNone
		if newWinner {
			c.winner = side
		}
		confirm := side == failoverRaceChallenger && !c.confirmed
		if confirm {
			c.confirmed = true
		}
		loser := c.otherPathLocked(side).conn
		onConfirmed := c.onConfirmed
		c.mu.Unlock()

		if newWinner {
			c.cancelDial()
			if loser != nil {
				_ = loser.Close()
			}
			c.releaseExtra()
			c.signal()
		}
		if confirm && onConfirmed != nil {
			onConfirmed()
		}
		return true
	}
}

func (c *firstIOFailoverConn) drainReads(results <-chan failoverRaceReadResult, active int) {
	for index := 0; index < active; index++ {
		<-results
	}
}

func (c *firstIOFailoverConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for attempts := 0; attempts < 2; attempts++ {
		side, conn, err := c.waitWritable()
		if err != nil {
			return 0, err
		}

		c.mu.Lock()
		if c.winner != failoverRaceNone {
			conn = c.pathLocked(c.winner).conn
			c.mu.Unlock()
			return conn.Write(p)
		}
		c.writeInProgress = true
		c.writeDone = make(chan struct{})
		c.mu.Unlock()

		n, writeErr := conn.Write(p)
		c.mu.Lock()
		c.writeInProgress = false
		close(c.writeDone)
		if n > 0 {
			c.winner = side
			loser := c.otherPathLocked(side).conn
			c.mu.Unlock()
			c.cancelDial()
			if loser != nil {
				_ = loser.Close()
			}
			c.releaseExtra()
			c.signal()
			return n, writeErr
		}
		c.mu.Unlock()
		c.signal()
		if writeErr == nil {
			return 0, nil
		}
		c.failPath(side, writeErr)
	}

	c.mu.Lock()
	err := c.combinedErrorLocked()
	c.mu.Unlock()
	return 0, err
}

func (c *firstIOFailoverConn) waitWritable() (failoverRaceSide, C.Conn, error) {
	for {
		c.mu.Lock()
		if c.winner != failoverRaceNone {
			path := c.pathLocked(c.winner)
			c.mu.Unlock()
			return c.winner, path.conn, nil
		}
		for _, side := range []failoverRaceSide{c.initial, failoverRacePrimary, failoverRaceChallenger} {
			if side == failoverRaceNone {
				continue
			}
			path := c.pathLocked(side)
			if path.conn != nil && !path.failed {
				c.mu.Unlock()
				return side, path.conn, nil
			}
		}
		noPotential := c.noPotentialLocked()
		err := c.combinedErrorLocked()
		c.mu.Unlock()
		if noPotential {
			return failoverRaceNone, nil, err
		}
		select {
		case c.triggerChallenger <- struct{}{}:
		default:
		}
		<-c.notify
	}
}

func (c *firstIOFailoverConn) failPath(side failoverRaceSide, err error) {
	c.mu.Lock()
	path := c.pathLocked(side)
	if c.winner == side {
		c.mu.Unlock()
		return
	}
	if c.winner == failoverRaceNone {
		path.failed = true
		path.err = err
	}
	conn := path.conn
	c.mu.Unlock()
	if side == failoverRacePrimary {
		select {
		case c.triggerChallenger <- struct{}{}:
		default:
		}
	}
	if conn != nil {
		_ = conn.Close()
	}
	c.signal()
}

func (c *firstIOFailoverConn) ReadBuffer(buffer *buf.Buffer) error {
	if conn := c.winnerConn(); conn != nil {
		return conn.ReadBuffer(buffer)
	}
	_, err := buffer.ReadOnceFrom(c)
	return err
}

func (c *firstIOFailoverConn) WriteBuffer(buffer *buf.Buffer) error {
	c.mu.Lock()
	winner := c.winner
	var conn C.Conn
	if winner != failoverRaceNone {
		conn = c.pathLocked(winner).conn
	}
	c.mu.Unlock()
	if conn != nil {
		return conn.WriteBuffer(buffer)
	}
	defer buffer.Release()
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *firstIOFailoverConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	primary := c.primary.conn
	challenger := c.challenger.conn
	c.mu.Unlock()
	c.cancelDial()
	var err error
	if primary != nil {
		err = primary.Close()
	}
	if challenger != nil && challenger != primary {
		if closeErr := challenger.Close(); err == nil {
			err = closeErr
		}
	}
	c.releaseExtra()
	c.signal()
	return err
}

func (c *firstIOFailoverConn) LocalAddr() net.Addr {
	return c.currentConn().LocalAddr()
}

func (c *firstIOFailoverConn) RemoteAddr() net.Addr {
	return c.currentConn().RemoteAddr()
}

func (c *firstIOFailoverConn) SetDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.mu.Lock()
	c.readDeadline, c.writeDeadline = t, t
	c.readDeadlineSet, c.writeDeadlineSet = true, true
	conns := c.connectionsLocked()
	c.mu.Unlock()
	var err error
	for _, conn := range conns {
		if setErr := conn.SetDeadline(t); err == nil {
			err = setErr
		}
	}
	return err
}

func (c *firstIOFailoverConn) SetReadDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.mu.Lock()
	c.readDeadline, c.readDeadlineSet = t, true
	conns := c.connectionsLocked()
	c.mu.Unlock()
	var err error
	for _, conn := range conns {
		if setErr := conn.SetReadDeadline(t); err == nil {
			err = setErr
		}
	}
	return err
}

func (c *firstIOFailoverConn) SetWriteDeadline(t time.Time) error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	c.mu.Lock()
	c.writeDeadline, c.writeDeadlineSet = t, true
	conns := c.connectionsLocked()
	c.mu.Unlock()
	var err error
	for _, conn := range conns {
		if setErr := conn.SetWriteDeadline(t); err == nil {
			err = setErr
		}
	}
	return err
}

func (c *firstIOFailoverConn) CloseRead() error {
	conn := c.currentConn()
	if closer, ok := S.Cast[SN.ReadCloser](conn); ok {
		return closer.CloseRead()
	}
	return conn.Close()
}

func (c *firstIOFailoverConn) CloseWrite() error {
	conn := c.currentConn()
	if closer, ok := S.Cast[SN.WriteCloser](conn); ok {
		return closer.CloseWrite()
	}
	return conn.Close()
}

func (c *firstIOFailoverConn) NeedHandshake() bool {
	return N.NeedHandshake(c.currentConn())
}

func (c *firstIOFailoverConn) ReaderReplaceable() bool { return false }
func (c *firstIOFailoverConn) WriterReplaceable() bool { return false }

func (c *firstIOFailoverConn) currentConn() C.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.winner != failoverRaceNone {
		return c.pathLocked(c.winner).conn
	}
	return c.pathLocked(c.initial).conn
}

func (c *firstIOFailoverConn) winnerConn() C.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.winner == failoverRaceNone {
		return nil
	}
	return c.pathLocked(c.winner).conn
}

func (c *firstIOFailoverConn) applyStoredDeadlinesLocked(conn C.Conn) {
	if c.readDeadlineSet {
		_ = conn.SetReadDeadline(c.readDeadline)
	}
	if c.writeDeadlineSet {
		_ = conn.SetWriteDeadline(c.writeDeadline)
	}
}

func (c *firstIOFailoverConn) pathLocked(side failoverRaceSide) *failoverRacePath {
	if side == failoverRaceChallenger {
		return &c.challenger
	}
	return &c.primary
}

func (c *firstIOFailoverConn) otherPathLocked(side failoverRaceSide) *failoverRacePath {
	if side == failoverRaceChallenger {
		return &c.primary
	}
	return &c.challenger
}

func (c *firstIOFailoverConn) noPotentialLocked() bool {
	for _, path := range []*failoverRacePath{&c.primary, &c.challenger} {
		if path.conn != nil && !path.failed || !path.dialDone {
			return false
		}
	}
	return true
}

func (c *firstIOFailoverConn) combinedErrorLocked() error {
	primaryErr := c.primary.err
	challengerErr := c.challenger.err
	if primaryErr == nil {
		primaryErr = errors.New("primary path unavailable")
	}
	if challengerErr == nil {
		return primaryErr
	}
	return errors.Join(
		fmt.Errorf("primary path failed: %w", primaryErr),
		fmt.Errorf("challenger path failed: %w", challengerErr),
	)
}

func (c *firstIOFailoverConn) connectionsLocked() []C.Conn {
	connections := make([]C.Conn, 0, 2)
	if c.primary.conn != nil {
		connections = append(connections, c.primary.conn)
	}
	if c.challenger.conn != nil && c.challenger.conn != c.primary.conn {
		connections = append(connections, c.challenger.conn)
	}
	return connections
}

func (c *firstIOFailoverConn) releaseExtra() {
	c.mu.Lock()
	release := c.releaseChallenger
	dialDone := c.challenger.dialDone
	c.mu.Unlock()
	if release == nil || !dialDone {
		return
	}
	c.releaseOnce.Do(func() {
		release()
	})
}

func (c *firstIOFailoverConn) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

var _ C.Conn = (*firstIOFailoverConn)(nil)
