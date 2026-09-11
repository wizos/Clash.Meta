package outboundgroup_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/adapter/outboundgroup"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

var failoverRaceSocketSequence atomic.Uint64

type failoverRaceSocketEndpoint struct {
	listener *net.TCPListener
	handler  func(*failoverRaceSocketEndpoint, *net.TCPConn)
	stop     chan struct{}
	accepted chan struct{}

	acceptWG sync.WaitGroup
	connWG   sync.WaitGroup
	close    sync.Once
	mu       sync.Mutex
	conns    map[*net.TCPConn]struct{}
	active   atomic.Int32
	payloads [][]byte
}

func newFailoverRaceSocketEndpoint(
	t *testing.T,
	handler func(*failoverRaceSocketEndpoint, *net.TCPConn),
) *failoverRaceSocketEndpoint {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &failoverRaceSocketEndpoint{
		listener: listener,
		handler:  handler,
		stop:     make(chan struct{}),
		accepted: make(chan struct{}, 16),
		conns:    map[*net.TCPConn]struct{}{},
	}
	endpoint.acceptWG.Add(1)
	go endpoint.acceptLoop()
	t.Cleanup(endpoint.Close)
	return endpoint
}

func (e *failoverRaceSocketEndpoint) acceptLoop() {
	defer e.acceptWG.Done()
	for {
		conn, err := e.listener.AcceptTCP()
		if err != nil {
			return
		}
		e.mu.Lock()
		e.conns[conn] = struct{}{}
		e.mu.Unlock()
		e.active.Add(1)
		e.connWG.Add(1)
		select {
		case e.accepted <- struct{}{}:
		default:
		}
		go func() {
			defer func() {
				_ = conn.Close()
				e.mu.Lock()
				delete(e.conns, conn)
				e.mu.Unlock()
				e.active.Add(-1)
				e.connWG.Done()
			}()
			e.handler(e, conn)
		}()
	}
}

func (e *failoverRaceSocketEndpoint) Addr() string {
	return e.listener.Addr().String()
}

func (e *failoverRaceSocketEndpoint) RecordPayload(payload []byte) {
	e.mu.Lock()
	e.payloads = append(e.payloads, append([]byte(nil), payload...))
	e.mu.Unlock()
}

func (e *failoverRaceSocketEndpoint) PayloadCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.payloads)
}

func (e *failoverRaceSocketEndpoint) Payloads() [][]byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	payloads := make([][]byte, len(e.payloads))
	for index, payload := range e.payloads {
		payloads[index] = append([]byte(nil), payload...)
	}
	return payloads
}

func (e *failoverRaceSocketEndpoint) Close() {
	e.close.Do(func() {
		close(e.stop)
		_ = e.listener.Close()
		e.acceptWG.Wait()
		e.mu.Lock()
		connections := make([]*net.TCPConn, 0, len(e.conns))
		for conn := range e.conns {
			connections = append(connections, conn)
		}
		e.mu.Unlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		e.connWG.Wait()
	})
}

type failoverRaceTrackedConn struct {
	net.Conn
	owner *failoverRaceSocketAdapter
	close sync.Once
}

func (c *failoverRaceTrackedConn) Close() error {
	err := c.Conn.Close()
	c.close.Do(func() {
		c.owner.active.Add(-1)
		c.owner.closeCalls.Add(1)
	})
	return err
}

func (c *failoverRaceTrackedConn) CloseRead() error {
	return c.Conn.(*net.TCPConn).CloseRead()
}

func (c *failoverRaceTrackedConn) CloseWrite() error {
	return c.Conn.(*net.TCPConn).CloseWrite()
}

func (c *failoverRaceTrackedConn) SyscallConn() (syscall.RawConn, error) {
	return c.Conn.(syscall.Conn).SyscallConn()
}

type failoverRaceSocketAdapter struct {
	*outbound.Base
	address             string
	beforeReturn        <-chan struct{}
	ignoreCancelOnBlock bool
	wrap                func(net.Conn) net.Conn
	dialCalls           atomic.Int32
	active              atomic.Int32
	closeCalls          atomic.Int32
	connected           chan struct{}
	returned            chan struct{}
	connectedOnce       sync.Once
	returnedOnce        sync.Once
}

func newFailoverRaceSocketAdapter(name, address string) *failoverRaceSocketAdapter {
	return &failoverRaceSocketAdapter{
		Base: outbound.NewBase(outbound.BaseOption{
			Name: name,
			Addr: address,
			Type: C.Direct,
		}),
		address:   address,
		connected: make(chan struct{}),
		returned:  make(chan struct{}),
	}
}

func (a *failoverRaceSocketAdapter) DialContext(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
	a.dialCalls.Add(1)
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp4", a.address)
	if err != nil {
		return nil, err
	}
	a.active.Add(1)
	tracked := &failoverRaceTrackedConn{Conn: conn, owner: a}
	a.connectedOnce.Do(func() { close(a.connected) })
	if a.beforeReturn != nil {
		if a.ignoreCancelOnBlock {
			<-a.beforeReturn
		} else {
			select {
			case <-a.beforeReturn:
			case <-ctx.Done():
				_ = tracked.Close()
				return nil, ctx.Err()
			}
		}
	}
	var wrapped net.Conn = tracked
	if a.wrap != nil {
		wrapped = a.wrap(wrapped)
	}
	a.returnedOnce.Do(func() { close(a.returned) })
	return outbound.NewConn(wrapped, a), nil
}

type failoverRaceFirstWriteConn struct {
	net.Conn
	first func(net.Conn, []byte) (int, error)
	used  atomic.Bool
}

func (c *failoverRaceFirstWriteConn) Write(payload []byte) (int, error) {
	if !c.used.Swap(true) {
		return c.first(c.Conn, payload)
	}
	return c.Conn.Write(payload)
}

func (c *failoverRaceFirstWriteConn) CloseRead() error {
	return c.Conn.(interface{ CloseRead() error }).CloseRead()
}

func (c *failoverRaceFirstWriteConn) CloseWrite() error {
	return c.Conn.(interface{ CloseWrite() error }).CloseWrite()
}

func (c *failoverRaceFirstWriteConn) SyscallConn() (syscall.RawConn, error) {
	return c.Conn.(syscall.Conn).SyscallConn()
}

type failoverRaceSocketGroup interface {
	C.ProxyAdapter
	Now() string
}

func newFailoverRaceSocketGroup(
	t *testing.T,
	groupType string,
	delay time.Duration,
	primaryAdapter *failoverRaceSocketAdapter,
	challengerAdapter *failoverRaceSocketAdapter,
) failoverRaceSocketGroup {
	t.Helper()
	primary := adapter.NewProxy(primaryAdapter)
	challenger := adapter.NewProxy(challengerAdapter)
	groupName := primary.Name() + "-group"
	providers := map[string]P.ProxyProvider{}
	parsed, err := outboundgroup.ParseProxyGroup(map[string]any{
		"name":           groupName,
		"type":           groupType,
		"proxies":        []string{primary.Name(), challenger.Name()},
		"empty-fallback": primary.Name(),
		"failover-race":  true,
	}, map[string]C.Proxy{
		primary.Name():    primary,
		challenger.Name(): challenger,
	}, providers, nil, nil, delay)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = parsed.Close()
		for _, provider := range providers {
			if closer, ok := provider.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	})
	group, ok := parsed.(failoverRaceSocketGroup)
	if !ok {
		t.Fatalf("parsed %s group does not expose Now: %T", groupType, parsed)
	}
	return group
}

func newFailoverRaceSocketNames(t *testing.T) (string, string) {
	t.Helper()
	sequence := failoverRaceSocketSequence.Add(1)
	return fmt.Sprintf("primary-%s-%d", t.Name(), sequence), fmt.Sprintf("challenger-%s-%d", t.Name(), sequence)
}

func failoverRaceSocketMetadata(sequence uint64) *C.Metadata {
	return &C.Metadata{
		NetWork: C.TCP,
		Host:    fmt.Sprintf("socket-race-%d.test", sequence),
		DstPort: uint16(10000 + sequence%50000),
	}
}

func waitFailoverRaceSocketEvent(t *testing.T, event <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func waitFailoverRaceSocketCondition(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal(message)
	}
}

func failoverRaceSocketPayloadHandler(
	expectedSize int,
) func(*failoverRaceSocketEndpoint, *net.TCPConn) {
	return func(endpoint *failoverRaceSocketEndpoint, conn *net.TCPConn) {
		payload := make([]byte, expectedSize)
		n, _ := io.ReadFull(conn, payload)
		if n > 0 {
			endpoint.RecordPayload(payload[:n])
		}
		_, _ = io.Copy(io.Discard, conn)
	}
}

func TestFailoverRaceSocketServerFirstChallengerWins(t *testing.T) {
	primaryRelease := make(chan struct{})
	primaryEndpoint := newFailoverRaceSocketEndpoint(t, func(endpoint *failoverRaceSocketEndpoint, conn *net.TCPConn) {
		select {
		case <-primaryRelease:
			_, _ = conn.Write([]byte("P"))
		case <-endpoint.stop:
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	})
	challengerEndpoint := newFailoverRaceSocketEndpoint(t, func(_ *failoverRaceSocketEndpoint, conn *net.TCPConn) {
		_, _ = conn.Write([]byte("C"))
		_, _ = io.Copy(io.Discard, conn)
	})
	primaryName, challengerName := newFailoverRaceSocketNames(t)
	primary := newFailoverRaceSocketAdapter(primaryName, primaryEndpoint.Addr())
	challenger := newFailoverRaceSocketAdapter(challengerName, challengerEndpoint.Addr())
	group := newFailoverRaceSocketGroup(t, "fallback", 10*time.Millisecond, primary, challenger)

	if now := group.Now(); now != primaryName {
		t.Fatalf("unexpected initial proxy: %s", now)
	}
	sequence := failoverRaceSocketSequence.Load()
	conn, err := group.DialContext(context.Background(), failoverRaceSocketMetadata(sequence))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	buffer := make([]byte, 1)
	n, err := conn.Read(buffer)
	if err != nil || n != 1 || buffer[0] != 'C' {
		t.Fatalf("unexpected first downstream byte: n=%d data=%q err=%v", n, buffer[:n], err)
	}
	if now := group.Now(); now != challengerName {
		t.Fatalf("challenger takeover was not committed: %s", now)
	}
	waitFailoverRaceSocketCondition(t, func() bool {
		return primary.active.Load() == 0 && primary.closeCalls.Load() == 1
	}, "primary socket was not closed after challenger won")
	close(primaryRelease)
}

func TestFailoverRaceSocketWinnerDeadlineAndHalfClose(t *testing.T) {
	primaryRelease := make(chan struct{})
	var primaryReleaseOnce sync.Once
	t.Cleanup(func() { primaryReleaseOnce.Do(func() { close(primaryRelease) }) })
	primaryEndpoint := newFailoverRaceSocketEndpoint(t, func(endpoint *failoverRaceSocketEndpoint, conn *net.TCPConn) {
		select {
		case <-primaryRelease:
			_, _ = conn.Write([]byte("P"))
		case <-endpoint.stop:
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	})
	halfCloseResult := make(chan error, 1)
	responseRelease := make(chan struct{})
	var responseReleaseOnce sync.Once
	t.Cleanup(func() { responseReleaseOnce.Do(func() { close(responseRelease) }) })
	challengerEndpoint := newFailoverRaceSocketEndpoint(t, func(endpoint *failoverRaceSocketEndpoint, conn *net.TCPConn) {
		_, _ = conn.Write([]byte("C"))
		buffer := make([]byte, 1)
		n, err := conn.Read(buffer)
		if n != 0 || !errors.Is(err, io.EOF) {
			halfCloseResult <- fmt.Errorf("server did not observe write EOF: n=%d err=%v", n, err)
			return
		}
		halfCloseResult <- nil
		select {
		case <-responseRelease:
			_, _ = conn.Write([]byte("E"))
		case <-endpoint.stop:
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	})
	primaryName, challengerName := newFailoverRaceSocketNames(t)
	primary := newFailoverRaceSocketAdapter(primaryName, primaryEndpoint.Addr())
	challenger := newFailoverRaceSocketAdapter(challengerName, challengerEndpoint.Addr())
	group := newFailoverRaceSocketGroup(t, "fallback", 10*time.Millisecond, primary, challenger)
	sequence := failoverRaceSocketSequence.Load()

	conn, err := group.DialContext(context.Background(), failoverRaceSocketMetadata(sequence))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	buffer := make([]byte, 1)
	if n, err := conn.Read(buffer); err != nil || n != 1 || buffer[0] != 'C' {
		t.Fatalf("challenger did not win before half-close: n=%d data=%q err=%v", n, buffer, err)
	}
	closeWriter, ok := conn.(interface{ CloseWrite() error })
	if !ok {
		t.Fatalf("race connection does not expose CloseWrite: %T", conn)
	}
	if err := closeWriter.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-halfCloseResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe the winner write half-close")
	}

	if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if n, err := conn.Read(buffer); n != 0 || err == nil {
		t.Fatalf("winner read deadline did not fire: n=%d err=%v", n, err)
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("winner read returned a non-timeout error: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	responseReleaseOnce.Do(func() { close(responseRelease) })
	if n, err := conn.Read(buffer); err != nil || n != 1 || buffer[0] != 'E' {
		t.Fatalf("read side did not survive CloseWrite: n=%d data=%q err=%v", n, buffer, err)
	}
	primaryReleaseOnce.Do(func() { close(primaryRelease) })
}

func TestFailoverRaceSocketFirstWriteIsNotReplayed(t *testing.T) {
	partialErr := errors.New("injected partial write")
	for _, testCase := range []struct {
		name          string
		written       int
		expectedError error
	}{
		{name: "complete", written: len("complete-request")},
		{name: "partial", written: 1, expectedError: partialErr},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			payload := []byte(testCase.name + "-request")
			primaryEndpoint := newFailoverRaceSocketEndpoint(t, failoverRaceSocketPayloadHandler(testCase.written))
			challengerEndpoint := newFailoverRaceSocketEndpoint(t, failoverRaceSocketPayloadHandler(len(payload)))
			primaryName, challengerName := newFailoverRaceSocketNames(t)
			primary := newFailoverRaceSocketAdapter(primaryName, primaryEndpoint.Addr())
			if testCase.expectedError != nil {
				primary.wrap = func(conn net.Conn) net.Conn {
					return &failoverRaceFirstWriteConn{
						Conn: conn,
						first: func(conn net.Conn, payload []byte) (int, error) {
							n, err := conn.Write(payload[:testCase.written])
							if err != nil {
								return n, err
							}
							return n, testCase.expectedError
						},
					}
				}
			}
			challengerGate := make(chan struct{})
			var challengerGateOnce sync.Once
			t.Cleanup(func() { challengerGateOnce.Do(func() { close(challengerGate) }) })
			challenger := newFailoverRaceSocketAdapter(challengerName, challengerEndpoint.Addr())
			challenger.beforeReturn = challengerGate
			challenger.ignoreCancelOnBlock = true
			group := newFailoverRaceSocketGroup(t, "fallback", 0, primary, challenger)
			sequence := failoverRaceSocketSequence.Load()

			conn, err := group.DialContext(context.Background(), failoverRaceSocketMetadata(sequence))
			if err != nil {
				t.Fatal(err)
			}
			waitFailoverRaceSocketEvent(t, primaryEndpoint.accepted, "primary socket was not accepted")
			waitFailoverRaceSocketEvent(t, challengerEndpoint.accepted, "challenger socket was not accepted")
			challengerGateOnce.Do(func() { close(challengerGate) })
			waitFailoverRaceSocketEvent(t, challenger.returned, "challenger dial did not return")

			n, err := conn.Write(payload)
			if n != testCase.written || !errors.Is(err, testCase.expectedError) {
				t.Fatalf("unexpected first write: n=%d err=%v", n, err)
			}
			waitFailoverRaceSocketCondition(t, func() bool {
				return primaryEndpoint.PayloadCount() == 1
			}, "primary did not receive the first write")
			if payloads := primaryEndpoint.Payloads(); len(payloads) != 1 || !bytes.Equal(payloads[0], payload[:testCase.written]) {
				t.Fatalf("unexpected primary payloads: %q", payloads)
			}
			if count := challengerEndpoint.PayloadCount(); count != 0 {
				t.Fatalf("first write was replayed to challenger: %d payloads", count)
			}
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatal(err)
			}
			waitFailoverRaceSocketCondition(t, func() bool {
				return primary.active.Load() == 0 && challenger.active.Load() == 0 &&
					primaryEndpoint.active.Load() == 0 && challengerEndpoint.active.Load() == 0
			}, "socket remained active after the first-write test")
			if count := challengerEndpoint.PayloadCount(); count != 0 {
				t.Fatalf("challenger recorded a late replay after quiescence: %d payloads", count)
			}
		})
	}
}

func TestFailoverRaceSocketZeroByteWriteUsesChallengerOnce(t *testing.T) {
	payload := []byte("zero-write-request")
	primaryEndpoint := newFailoverRaceSocketEndpoint(t, failoverRaceSocketPayloadHandler(len(payload)))
	challengerEndpoint := newFailoverRaceSocketEndpoint(t, failoverRaceSocketPayloadHandler(len(payload)))
	primaryName, challengerName := newFailoverRaceSocketNames(t)
	primary := newFailoverRaceSocketAdapter(primaryName, primaryEndpoint.Addr())
	zeroWriteErr := errors.New("injected zero-byte write")
	primary.wrap = func(conn net.Conn) net.Conn {
		return &failoverRaceFirstWriteConn{
			Conn: conn,
			first: func(net.Conn, []byte) (int, error) {
				return 0, zeroWriteErr
			},
		}
	}
	challengerGate := make(chan struct{})
	var challengerGateOnce sync.Once
	t.Cleanup(func() { challengerGateOnce.Do(func() { close(challengerGate) }) })
	challenger := newFailoverRaceSocketAdapter(challengerName, challengerEndpoint.Addr())
	challenger.beforeReturn = challengerGate
	challenger.ignoreCancelOnBlock = true
	group := newFailoverRaceSocketGroup(t, "fallback", 0, primary, challenger)
	sequence := failoverRaceSocketSequence.Load()

	conn, err := group.DialContext(context.Background(), failoverRaceSocketMetadata(sequence))
	if err != nil {
		t.Fatal(err)
	}
	waitFailoverRaceSocketEvent(t, primaryEndpoint.accepted, "primary socket was not accepted")
	waitFailoverRaceSocketEvent(t, challengerEndpoint.accepted, "challenger socket was not accepted")
	challengerGateOnce.Do(func() { close(challengerGate) })
	waitFailoverRaceSocketEvent(t, challenger.returned, "challenger dial did not return")

	n, err := conn.Write(payload)
	if err != nil || n != len(payload) {
		t.Fatalf("challenger did not complete the retried write: n=%d err=%v", n, err)
	}
	waitFailoverRaceSocketCondition(t, func() bool {
		return challengerEndpoint.PayloadCount() == 1
	}, "challenger did not receive the retried payload")
	if count := primaryEndpoint.PayloadCount(); count != 0 {
		t.Fatalf("zero-byte primary write reached the server: %d payloads", count)
	}
	if payloads := challengerEndpoint.Payloads(); len(payloads) != 1 || !bytes.Equal(payloads[0], payload) {
		t.Fatalf("unexpected challenger payloads: %q", payloads)
	}
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	waitFailoverRaceSocketCondition(t, func() bool {
		return primary.active.Load() == 0 && challenger.active.Load() == 0 &&
			primaryEndpoint.active.Load() == 0 && challengerEndpoint.active.Load() == 0
	}, "socket remained active after the zero-byte write test")
	if count := primaryEndpoint.PayloadCount(); count != 0 {
		t.Fatalf("primary recorded a late payload after quiescence: %d payloads", count)
	}
}

func TestFailoverRaceSocketCloseAndContextCleanLateDials(t *testing.T) {
	baseline := runtime.NumGoroutine()
	for _, action := range []string{"close", "context"} {
		t.Run(action, func(t *testing.T) {
			primaryEndpoint := newFailoverRaceSocketEndpoint(t, func(_ *failoverRaceSocketEndpoint, conn *net.TCPConn) {
				_, _ = io.Copy(io.Discard, conn)
			})
			challengerEndpoint := newFailoverRaceSocketEndpoint(t, func(_ *failoverRaceSocketEndpoint, conn *net.TCPConn) {
				_, _ = io.Copy(io.Discard, conn)
			})
			primaryName, challengerName := newFailoverRaceSocketNames(t)
			primary := newFailoverRaceSocketAdapter(primaryName, primaryEndpoint.Addr())
			challenger := newFailoverRaceSocketAdapter(challengerName, challengerEndpoint.Addr())
			challengerGate := make(chan struct{})
			var challengerGateOnce sync.Once
			t.Cleanup(func() { challengerGateOnce.Do(func() { close(challengerGate) }) })
			challenger.beforeReturn = challengerGate
			challenger.ignoreCancelOnBlock = true
			var primaryGate chan struct{}
			var primaryGateOnce sync.Once
			if action == "context" {
				primaryGate = make(chan struct{})
				primary.beforeReturn = primaryGate
				primary.ignoreCancelOnBlock = true
				t.Cleanup(func() { primaryGateOnce.Do(func() { close(primaryGate) }) })
			}
			group := newFailoverRaceSocketGroup(t, "fallback", 0, primary, challenger)
			sequence := failoverRaceSocketSequence.Load()

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan struct {
				conn C.Conn
				err  error
			}, 1)
			go func() {
				conn, err := group.DialContext(ctx, failoverRaceSocketMetadata(sequence))
				result <- struct {
					conn C.Conn
					err  error
				}{conn: conn, err: err}
			}()
			waitFailoverRaceSocketEvent(t, primaryEndpoint.accepted, "primary late socket was not accepted")
			waitFailoverRaceSocketEvent(t, challengerEndpoint.accepted, "challenger late socket was not accepted")
			waitFailoverRaceSocketEvent(t, primary.connected, "primary client socket did not connect")
			waitFailoverRaceSocketEvent(t, challenger.connected, "challenger client socket did not connect")

			if action == "close" {
				outcome := <-result
				if outcome.err != nil {
					t.Fatal(outcome.err)
				}
				if err := outcome.conn.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
				outcome := <-result
				if outcome.conn != nil {
					_ = outcome.conn.Close()
				}
				if !errors.Is(outcome.err, context.Canceled) {
					t.Fatalf("unexpected canceled dial error: %v", outcome.err)
				}
			}
			cancel()
			challengerGateOnce.Do(func() { close(challengerGate) })
			if primaryGate != nil {
				primaryGateOnce.Do(func() { close(primaryGate) })
			}
			waitFailoverRaceSocketEvent(t, primary.returned, "primary late dial did not return")
			waitFailoverRaceSocketEvent(t, challenger.returned, "challenger late dial did not return")
			waitFailoverRaceSocketCondition(t, func() bool {
				return primary.active.Load() == 0 && challenger.active.Load() == 0 &&
					primaryEndpoint.active.Load() == 0 && challengerEndpoint.active.Load() == 0
			}, "late socket remained active after close or cancellation")
		})
	}
	runtime.GC()
	waitFailoverRaceSocketCondition(t, func() bool {
		return runtime.NumGoroutine() <= baseline+4
	}, fmt.Sprintf("goroutines did not return near baseline: before=%d after=%d", baseline, runtime.NumGoroutine()))
}
