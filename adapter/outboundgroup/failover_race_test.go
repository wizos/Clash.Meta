package outboundgroup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type failoverRaceTestProxy struct {
	C.Proxy
	name      string
	alive     bool
	delay     uint16
	dial      func(context.Context) (C.Conn, error)
	dialCalls atomic.Int32
}

func (p *failoverRaceTestProxy) Adapter() C.ProxyAdapter { return nil }

func (p *failoverRaceTestProxy) Name() string        { return p.name }
func (p *failoverRaceTestProxy) Type() C.AdapterType { return C.Direct }
func (p *failoverRaceTestProxy) Addr() string        { return p.name }
func (p *failoverRaceTestProxy) ProxyInfo() C.ProxyInfo {
	return C.ProxyInfo{}
}
func (p *failoverRaceTestProxy) AliveForTestUrl(string) bool { return p.alive }
func (p *failoverRaceTestProxy) LastDelayForTestUrl(string) uint16 {
	return p.delay
}
func (p *failoverRaceTestProxy) Unwrap(*C.Metadata, bool) C.Proxy { return nil }
func (p *failoverRaceTestProxy) DialContext(ctx context.Context, _ *C.Metadata) (C.Conn, error) {
	p.dialCalls.Add(1)
	return p.dial(ctx)
}

type failoverRaceTestProvider struct {
	P.ProxyProvider
	proxies []C.Proxy
}

func (p *failoverRaceTestProvider) Version() uint32    { return 1 }
func (p *failoverRaceTestProvider) Touch()             {}
func (p *failoverRaceTestProvider) Proxies() []C.Proxy { return p.proxies }

type failoverRaceTestRead struct {
	data []byte
	err  error
}

type failoverRaceTestConn struct {
	readResults chan failoverRaceTestRead
	closed      chan struct{}
	closeOnce   sync.Once
	closeCalls  atomic.Int32
	readCalls   atomic.Int32
	readBuffers atomic.Int32
	writeCalls  atomic.Int32
	write       func([]byte) (int, error)
	chain       C.Chain
}

func newFailoverRaceTestConn() *failoverRaceTestConn {
	return &failoverRaceTestConn{
		readResults: make(chan failoverRaceTestRead, 1),
		closed:      make(chan struct{}),
		write: func(p []byte) (int, error) {
			return len(p), nil
		},
	}
}

func (c *failoverRaceTestConn) Read(p []byte) (int, error) {
	c.readCalls.Add(1)
	select {
	case result := <-c.readResults:
		return copy(p, result.data), result.err
	case <-c.closed:
		return 0, net.ErrClosed
	}
}

func (c *failoverRaceTestConn) Write(p []byte) (int, error) {
	c.writeCalls.Add(1)
	return c.write(p)
}

func (c *failoverRaceTestConn) ReadBuffer(buffer *buf.Buffer) error {
	c.readBuffers.Add(1)
	_, err := buffer.ReadOnceFrom(c)
	return err
}

func (c *failoverRaceTestConn) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *failoverRaceTestConn) Close() error {
	c.closeCalls.Add(1)
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *failoverRaceTestConn) LocalAddr() net.Addr              { return failoverRaceTestAddr("local") }
func (c *failoverRaceTestConn) RemoteAddr() net.Addr             { return failoverRaceTestAddr("remote") }
func (c *failoverRaceTestConn) SetDeadline(time.Time) error      { return nil }
func (c *failoverRaceTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *failoverRaceTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *failoverRaceTestConn) Chains() C.Chain                  { return c.chain }
func (c *failoverRaceTestConn) ProviderChains() C.Chain          { return nil }
func (c *failoverRaceTestConn) AppendToChains(adapter C.ProxyAdapter) {
	c.chain = append(c.chain, adapter.Name())
}
func (c *failoverRaceTestConn) RemoteDestination() string { return "remote" }

type failoverRaceTestAddr string

func (a failoverRaceTestAddr) Network() string { return "test" }
func (a failoverRaceTestAddr) String() string  { return string(a) }

type failoverRaceDeadlineConn struct {
	*failoverRaceTestConn
	deadlineMu     sync.Mutex
	readDeadlines  []time.Time
	writeDeadlines []time.Time
	hook           func(string, time.Time)
}

func (c *failoverRaceDeadlineConn) SetDeadline(deadline time.Time) error {
	if c.hook != nil {
		c.hook("overall", deadline)
	}
	c.deadlineMu.Lock()
	c.readDeadlines = append(c.readDeadlines, deadline)
	c.writeDeadlines = append(c.writeDeadlines, deadline)
	c.deadlineMu.Unlock()
	return nil
}

func (c *failoverRaceDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if c.hook != nil {
		c.hook("read", deadline)
	}
	c.deadlineMu.Lock()
	c.readDeadlines = append(c.readDeadlines, deadline)
	c.deadlineMu.Unlock()
	return nil
}

func (c *failoverRaceDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	if c.hook != nil {
		c.hook("write", deadline)
	}
	c.deadlineMu.Lock()
	c.writeDeadlines = append(c.writeDeadlines, deadline)
	c.deadlineMu.Unlock()
	return nil
}

func (c *failoverRaceDeadlineConn) lastDeadlines() (time.Time, time.Time) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	var readDeadline, writeDeadline time.Time
	if len(c.readDeadlines) != 0 {
		readDeadline = c.readDeadlines[len(c.readDeadlines)-1]
	}
	if len(c.writeDeadlines) != 0 {
		writeDeadline = c.writeDeadlines[len(c.writeDeadlines)-1]
	}
	return readDeadline, writeDeadline
}

type failoverRaceTypedError struct{ message string }

func (e *failoverRaceTypedError) Error() string { return e.message }

func resetFailoverRaceTestState(t *testing.T) {
	t.Helper()
	if used := len(failoverRaceExtra); used != 0 {
		t.Fatalf("failover race permits leaked before test: %d", used)
	}
	failoverRaceSlots.Lock()
	failoverRaceSlots.entries = map[string]failoverRaceSlot{}
	failoverRaceSlots.Unlock()
}

func failoverRaceTestMetadata(port uint16) *C.Metadata {
	return &C.Metadata{NetWork: C.TCP, Host: "example.test", DstPort: port}
}

func failoverRaceTestDial(ctx context.Context, proxy C.Proxy, _ *C.Metadata) (C.Conn, error) {
	return proxy.(*failoverRaceTestProxy).DialContext(ctx, nil)
}

func TestFailoverRaceDisabledUsesOriginalSingleDial(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryConn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return newFailoverRaceTestConn(), nil
	}}
	provider := &failoverRaceTestProvider{proxies: []C.Proxy{primary, challenger}}
	group, err := NewFallback(GroupCommonOption{Name: "group", URL: "test", FailoverRace: false}, FallbackOption{}, primary, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := group.DialContext(context.Background(), failoverRaceTestMetadata(1))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if primary.dialCalls.Load() != 1 || challenger.dialCalls.Load() != 0 {
		t.Fatalf("unexpected dial calls: primary=%d challenger=%d", primary.dialCalls.Load(), challenger.dialCalls.Load())
	}
	encoded, err := group.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("failover")) {
		t.Fatalf("group API changed: %s", encoded)
	}
}

func TestFailoverRaceDisabledPreservesFallbackInvalidFixedOrder(t *testing.T) {
	resetFailoverRaceTestState(t)
	first := &failoverRaceTestProxy{name: "first", alive: true}
	fixed := &failoverRaceTestProxy{name: "fixed", alive: false}
	third := &failoverRaceTestProxy{name: "third", alive: true}
	provider := &failoverRaceTestProvider{proxies: []C.Proxy{first, fixed, third}}
	group, err := NewFallback(GroupCommonOption{Name: "group", URL: "test", FailoverRace: false}, FallbackOption{}, first, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}
	group.ForceSet(fixed.Name())

	if now := group.Now(); now != third.Name() {
		t.Fatalf("invalid fixed node resumed from wrong position: %s", now)
	}
}

func TestFailoverRaceParserScopesOptionToAutomaticGroups(t *testing.T) {
	resetFailoverRaceTestState(t)
	newProxy := func(name string) *failoverRaceTestProxy {
		return &failoverRaceTestProxy{name: name, alive: true, dial: func(context.Context) (C.Conn, error) {
			return newFailoverRaceTestConn(), nil
		}}
	}
	compatible := newProxy("COMPATIBLE")
	primary := newProxy("primary")
	challenger := newProxy("challenger")
	proxyMap := map[string]C.Proxy{
		"COMPATIBLE": compatible,
		"primary":    primary,
		"challenger": challenger,
	}

	parsed, err := ParseProxyGroup(map[string]any{
		"name":          "fallback",
		"type":          "fallback",
		"proxies":       []string{"primary", "challenger"},
		"failover-race": true,
	}, proxyMap, map[string]P.ProxyProvider{}, nil, nil, 37*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	fallback, ok := parsed.(*Fallback)
	if !ok || !fallback.failoverRace || fallback.failoverDelay != 37*time.Millisecond {
		t.Fatalf("fallback race option not parsed: %#v", parsed)
	}

	parsed, err = ParseProxyGroup(map[string]any{
		"name":          "selector",
		"type":          "select",
		"proxies":       []string{"primary", "challenger"},
		"failover-race": true,
	}, proxyMap, map[string]P.ProxyProvider{}, nil, nil, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := parsed.DialContext(context.Background(), failoverRaceTestMetadata(9))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if primary.dialCalls.Load() != 1 || challenger.dialCalls.Load() != 0 {
		t.Fatalf("select group did not ignore failover-race: primary=%d challenger=%d", primary.dialCalls.Load(), challenger.dialCalls.Load())
	}
}

func TestFailoverRaceChallengerNeedsFirstDownstreamByte(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryConn := newFailoverRaceTestConn()
	challengerConn := newFailoverRaceTestConn()
	challengerConn.readResults <- failoverRaceTestRead{data: []byte("c")}
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return challengerConn, nil
	}}
	var confirmed atomic.Int32
	conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(2), primary, challenger, time.Millisecond, failoverRaceTestDial, func() {
		confirmed.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(10 * time.Millisecond)
	if confirmed.Load() != 0 {
		t.Fatal("connect completion committed challenger")
	}
	buffer := make([]byte, 1)
	n, err := conn.Read(buffer)
	if err != nil || n != 1 || buffer[0] != 'c' {
		t.Fatalf("unexpected read: n=%d data=%q err=%v", n, buffer[:n], err)
	}
	if confirmed.Load() != 1 {
		t.Fatalf("challenger confirmation count=%d", confirmed.Load())
	}
	if primaryConn.closeCalls.Load() == 0 {
		t.Fatal("primary loser was not closed")
	}
}

func TestFailoverRaceWinnerReadsUseUnderlyingExtendedConn(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryConn := newFailoverRaceTestConn()
	challengerConn := newFailoverRaceTestConn()
	challengerConn.readResults <- failoverRaceTestRead{data: []byte("first")}
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return challengerConn, nil
	}}
	conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(10), primary, challenger, 0, failoverRaceTestDial, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	buffer := make([]byte, 8)
	if _, err := conn.Read(buffer); err != nil {
		t.Fatal(err)
	}

	readCalls := challengerConn.readCalls.Load()
	challengerConn.readResults <- failoverRaceTestRead{data: []byte("next")}
	if _, err := conn.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if calls := challengerConn.readCalls.Load() - readCalls; calls != 1 {
		t.Fatalf("winner Read did not directly call underlying conn: %d", calls)
	}

	challengerConn.readResults <- failoverRaceTestRead{data: []byte("buffer")}
	extendedBuffer := buf.NewSize(16)
	defer extendedBuffer.Release()
	if err := conn.ReadBuffer(extendedBuffer); err != nil {
		t.Fatal(err)
	}
	if calls := challengerConn.readBuffers.Load(); calls != 1 {
		t.Fatalf("winner ReadBuffer bypassed underlying ExtendedConn: %d", calls)
	}
}

func TestFailoverRaceWinnerAndCloseDoNotWaitForLateDial(t *testing.T) {
	for _, action := range []string{"read", "write", "close"} {
		t.Run(action, func(t *testing.T) {
			resetFailoverRaceTestState(t)
			primaryConn := newFailoverRaceTestConn()
			primaryConn.readResults <- failoverRaceTestRead{data: []byte("p")}
			lateConn := newFailoverRaceTestConn()
			lateStarted := make(chan struct{})
			allowLateReturn := make(chan struct{})
			var startedOnce sync.Once
			primary := &failoverRaceTestProxy{name: "primary-" + action, alive: true, dial: func(context.Context) (C.Conn, error) {
				return primaryConn, nil
			}}
			challenger := &failoverRaceTestProxy{name: "challenger-" + action, alive: true, dial: func(context.Context) (C.Conn, error) {
				startedOnce.Do(func() { close(lateStarted) })
				<-allowLateReturn // Deliberately ignores context cancellation.
				return lateConn, nil
			}}
			conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(11), primary, challenger, 0, failoverRaceTestDial, nil)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-lateStarted:
			case <-time.After(time.Second):
				t.Fatal("challenger dial did not start")
			}

			actionDone := make(chan error, 1)
			go func() {
				switch action {
				case "read":
					_, err := conn.Read(make([]byte, 1))
					actionDone <- err
				case "write":
					_, err := conn.Write([]byte("request"))
					actionDone <- err
				default:
					actionDone <- conn.Close()
				}
			}()
			select {
			case err := <-actionDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("%s waited for a dial that ignored cancellation", action)
			}
			if used := len(failoverRaceExtra); used != 1 {
				t.Fatalf("late dial permit released before dial exit: %d", used)
			}
			close(allowLateReturn)
			select {
			case <-lateConn.closed:
			case <-time.After(time.Second):
				raced := conn.(*firstIOFailoverConn)
				raced.mu.Lock()
				winner, closed := raced.winner, raced.closed
				challengerDone, challengerPublished := raced.challenger.dialDone, raced.challenger.conn != nil
				raced.mu.Unlock()
				t.Fatalf("late connection was not closed: winner=%d closed=%v challengerDone=%v challengerPublished=%v", winner, closed, challengerDone, challengerPublished)
			}
			deadline := time.Now().Add(time.Second)
			for len(failoverRaceExtra) != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if used := len(failoverRaceExtra); used != 0 {
				t.Fatalf("late dial permit leaked: %d", used)
			}
			if action != "close" {
				_ = conn.Close()
			}
		})
	}
}

func TestFailoverRaceDeadlinePublicationIsLinearized(t *testing.T) {
	testCases := []struct {
		name        string
		blockedKind string
		set         func(*firstIOFailoverConn, time.Time) error
		check       func(time.Time, time.Time, time.Time) bool
	}{
		{
			name:        "read",
			blockedKind: "read",
			set:         (*firstIOFailoverConn).SetReadDeadline,
			check: func(expected, read, _ time.Time) bool {
				return read.Equal(expected)
			},
		},
		{
			name:        "write",
			blockedKind: "write",
			set:         (*firstIOFailoverConn).SetWriteDeadline,
			check: func(expected, _, write time.Time) bool {
				return write.Equal(expected)
			},
		},
		{
			name:        "overall",
			blockedKind: "read",
			set:         (*firstIOFailoverConn).SetDeadline,
			check: func(expected, read, write time.Time) bool {
				return read.Equal(expected) && write.Equal(expected)
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			oldDeadline := time.Unix(100, 0)
			newDeadline := time.Unix(200, 0)
			enteredOldApply := make(chan struct{})
			allowOldApply := make(chan struct{})
			var enterOnce sync.Once
			conn := &failoverRaceDeadlineConn{failoverRaceTestConn: newFailoverRaceTestConn()}
			conn.hook = func(kind string, deadline time.Time) {
				if kind == testCase.blockedKind && deadline.Equal(oldDeadline) {
					enterOnce.Do(func() { close(enteredOldApply) })
					<-allowOldApply
				}
			}
			raced := &firstIOFailoverConn{
				primary:    failoverRacePath{},
				challenger: failoverRacePath{dialDone: true},
				notify:     make(chan struct{}, 1),
			}
			if err := testCase.set(raced, oldDeadline); err != nil {
				t.Fatal(err)
			}
			finishDone := make(chan struct{})
			go func() {
				raced.finishDial(failoverRacePrimary, conn, nil)
				close(finishDone)
			}()
			select {
			case <-enteredOldApply:
			case <-time.After(time.Second):
				t.Fatal("late connection did not start applying stored deadline")
			}

			setterStarted := make(chan struct{})
			setterDone := make(chan error, 1)
			go func() {
				close(setterStarted)
				setterDone <- testCase.set(raced, newDeadline)
			}()
			<-setterStarted
			select {
			case err := <-setterDone:
				t.Fatalf("new deadline passed an unpublished connection: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(allowOldApply)
			select {
			case <-finishDone:
			case <-time.After(time.Second):
				t.Fatal("connection publication did not finish")
			}
			select {
			case err := <-setterDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("new deadline did not finish")
			}
			readDeadline, writeDeadline := conn.lastDeadlines()
			if !testCase.check(newDeadline, readDeadline, writeDeadline) {
				t.Fatalf("stale deadline won: read=%v write=%v", readDeadline, writeDeadline)
			}
		})
	}
}

func TestFailoverRacePartialWriteLocksWithoutReplay(t *testing.T) {
	resetFailoverRaceTestState(t)
	partialErr := errors.New("partial write")
	primaryConn := newFailoverRaceTestConn()
	primaryConn.write = func([]byte) (int, error) { return 1, partialErr }
	challengerConn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return challengerConn, nil
	}}
	conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(3), primary, challenger, time.Millisecond, failoverRaceTestDial, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(10 * time.Millisecond)
	n, err := conn.Write([]byte("request"))
	if n != 1 || !errors.Is(err, partialErr) {
		t.Fatalf("unexpected write: n=%d err=%v", n, err)
	}
	if primaryConn.writeCalls.Load() != 1 || challengerConn.writeCalls.Load() != 0 {
		t.Fatalf("write was replayed: primary=%d challenger=%d", primaryConn.writeCalls.Load(), challengerConn.writeCalls.Load())
	}
}

func TestFailoverRaceZeroByteWriteMayUseChallenger(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryErr := errors.New("zero-byte write")
	primaryConn := newFailoverRaceTestConn()
	primaryConn.write = func([]byte) (int, error) { return 0, primaryErr }
	challengerConn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return challengerConn, nil
	}}
	conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(5), primary, challenger, time.Millisecond, failoverRaceTestDial, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(10 * time.Millisecond)
	n, err := conn.Write([]byte("request"))
	if err != nil || n != len("request") {
		t.Fatalf("unexpected challenger write: n=%d err=%v", n, err)
	}
	if primaryConn.writeCalls.Load() != 1 || challengerConn.writeCalls.Load() != 1 {
		t.Fatalf("unexpected write calls: primary=%d challenger=%d", primaryConn.writeCalls.Load(), challengerConn.writeCalls.Load())
	}
}

func TestFailoverRaceManualEpochPreventsTakeover(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryConn := newFailoverRaceTestConn()
	challengerConn := newFailoverRaceTestConn()
	challengerConn.readResults <- failoverRaceTestRead{data: []byte("c")}
	primary := &failoverRaceTestProxy{name: "primary", alive: true, delay: 10, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, delay: 20, dial: func(context.Context) (C.Conn, error) {
		return challengerConn, nil
	}}
	provider := &failoverRaceTestProvider{proxies: []C.Proxy{primary, challenger}}
	group, err := NewFallback(GroupCommonOption{Name: "group", URL: "test", FailoverRace: true, FailoverRaceDelay: time.Millisecond}, FallbackOption{}, primary, []P.ProxyProvider{provider})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := group.DialContext(context.Background(), failoverRaceTestMetadata(6))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(10 * time.Millisecond)
	group.ForceSet(primary.Name())
	buffer := make([]byte, 1)
	if _, err := conn.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if now := group.Now(); now != primary.Name() {
		t.Fatalf("stale race overwrote manual selection: %s", now)
	}
}

func TestFailoverRaceTakeoverRequiresReadForAutomaticGroups(t *testing.T) {
	for _, groupType := range []string{"fallback", "url-test"} {
		t.Run(groupType, func(t *testing.T) {
			resetFailoverRaceTestState(t)
			primaryConn := newFailoverRaceTestConn()
			challengerConn := newFailoverRaceTestConn()
			challengerConn.readResults <- failoverRaceTestRead{data: []byte("c")}
			primary := &failoverRaceTestProxy{name: "primary-" + groupType, alive: true, delay: 10, dial: func(context.Context) (C.Conn, error) {
				return primaryConn, nil
			}}
			challenger := &failoverRaceTestProxy{name: "challenger-" + groupType, alive: true, delay: 20, dial: func(context.Context) (C.Conn, error) {
				return challengerConn, nil
			}}
			provider := &failoverRaceTestProvider{proxies: []C.Proxy{primary, challenger}}
			option := &GroupCommonOption{Name: "group-" + groupType, URL: "test", FailoverRace: true, FailoverRaceDelay: time.Millisecond}
			var group ProxyGroup
			var err error
			if groupType == "fallback" {
				group, err = NewFallback(*option, FallbackOption{}, primary, []P.ProxyProvider{provider})
			} else {
				group, err = NewURLTest(*option, URLTestOption{}, primary, []P.ProxyProvider{provider})
			}
			if err != nil {
				t.Fatal(err)
			}

			conn, err := group.DialContext(context.Background(), failoverRaceTestMetadata(7))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			time.Sleep(10 * time.Millisecond)
			if now := group.Now(); now != primary.Name() {
				t.Fatalf("connect alone changed group: %s", now)
			}
			buffer := make([]byte, 1)
			if _, err := conn.Read(buffer); err != nil {
				t.Fatal(err)
			}
			if now := group.Now(); now != challenger.Name() {
				t.Fatalf("read-confirmed takeover not committed: %s", now)
			}
		})
	}
}

func TestFailoverRaceGlobalLimitAndSuppression(t *testing.T) {
	resetFailoverRaceTestState(t)
	releases := make([]func(), 0, failoverRaceLimit)
	for index := 0; index < failoverRaceLimit; index++ {
		release, acquired := acquireFailoverRaceSlot(string(rune(index + 1)))
		if !acquired {
			t.Fatalf("permit %d was unexpectedly rejected", index)
		}
		releases = append(releases, release)
	}
	if _, acquired := acquireFailoverRaceSlot("overflow"); acquired {
		t.Fatal("65th challenger acquired a global permit")
	}
	for _, release := range releases {
		release()
	}
	if used := len(failoverRaceExtra); used != 0 {
		t.Fatalf("permits not released: %d", used)
	}
	if _, acquired := acquireFailoverRaceSlot(string(rune(1))); acquired {
		t.Fatal("same leaf and target was not suppressed")
	}
	failoverRaceSlots.Lock()
	failoverRaceSlots.entries[string(rune(1))] = failoverRaceSlot{until: time.Now().Add(-time.Second)}
	failoverRaceSlots.Unlock()
	release, acquired := acquireFailoverRaceSlot(string(rune(1)))
	if !acquired {
		t.Fatal("expired suppression was not released")
	}
	release()
}

func TestFailoverRaceCloseCancelsDelayedChallenger(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryConn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return primaryConn, nil
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return newFailoverRaceTestConn(), nil
	}}
	conn, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(8), primary, challenger, time.Hour, failoverRaceTestDial, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if challenger.dialCalls.Load() != 0 {
		t.Fatalf("delayed challenger dialed after close: %d", challenger.dialCalls.Load())
	}
	if used := len(failoverRaceExtra); used != 0 {
		t.Fatalf("permit leaked after close: %d", used)
	}
}

func TestFailoverRaceAggregatesDialFailures(t *testing.T) {
	resetFailoverRaceTestState(t)
	primaryErr := errors.New("primary failed")
	challengerErr := &failoverRaceTypedError{message: "challenger failed"}
	primary := &failoverRaceTestProxy{name: "primary", alive: true, dial: func(context.Context) (C.Conn, error) {
		return nil, primaryErr
	}}
	challenger := &failoverRaceTestProxy{name: "challenger", alive: true, dial: func(context.Context) (C.Conn, error) {
		return nil, challengerErr
	}}
	_, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(4), primary, challenger, time.Hour, failoverRaceTestDial, nil)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte(primaryErr.Error())) || !bytes.Contains([]byte(err.Error()), []byte(challengerErr.Error())) {
		t.Fatalf("unexpected combined error: %v", err)
	}
	if !errors.Is(err, primaryErr) || !errors.Is(err, challengerErr) {
		t.Fatalf("combined error cannot unwrap both paths: %v", err)
	}
	var typedErr *failoverRaceTypedError
	if !errors.As(err, &typedErr) || typedErr != challengerErr {
		t.Fatalf("combined error lost typed challenger error: %v", err)
	}
}

var _ C.Conn = (*failoverRaceTestConn)(nil)
var _ io.Reader = (*failoverRaceTestConn)(nil)

func TestFailoverRaceWriteThenReadConfirmsChallenger(t *testing.T) {
	resetFailoverRaceTestState(t)
	conn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "review-primary", alive: true, dial: func(context.Context) (C.Conn, error) { return nil, errors.New("primary failed") }}
	challenger := &failoverRaceTestProxy{name: "review-challenger", alive: true, dial: func(context.Context) (C.Conn, error) { return conn, nil }}
	var confirmed atomic.Int32
	raced, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(10443), primary, challenger, time.Second, failoverRaceTestDial, func() { confirmed.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer raced.Close()
	if n, err := raced.Write([]byte("request")); err != nil || n != 7 {
		t.Fatalf("write: %d %v", n, err)
	}
	conn.readResults <- failoverRaceTestRead{data: []byte("response")}
	out := make([]byte, 8)
	if n, err := raced.Read(out); err != nil || n != 8 {
		t.Fatalf("read: %d %v", n, err)
	}
	if got := confirmed.Load(); got != 1 {
		t.Fatalf("expected one confirmation, got %d", got)
	}
	conn.readResults <- failoverRaceTestRead{data: []byte("again")}
	buffer := buf.NewSize(16)
	defer buffer.Release()
	if err := raced.ReadBuffer(buffer); err != nil {
		t.Fatal(err)
	}
	if got := confirmed.Load(); got != 1 {
		t.Fatalf("duplicate confirmation: %d", got)
	}
}

func TestFailoverRaceWriteThenReadBufferConfirmsChallenger(t *testing.T) {
	resetFailoverRaceTestState(t)
	conn := newFailoverRaceTestConn()
	primary := &failoverRaceTestProxy{name: "review-primary", alive: true, dial: func(context.Context) (C.Conn, error) { return nil, errors.New("primary failed") }}
	challenger := &failoverRaceTestProxy{name: "review-challenger", alive: true, dial: func(context.Context) (C.Conn, error) { return conn, nil }}
	var confirmed atomic.Int32
	raced, err := dialFirstIOFailover(context.Background(), failoverRaceTestMetadata(10444), primary, challenger, time.Second, failoverRaceTestDial, func() { confirmed.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer raced.Close()
	if n, err := raced.Write([]byte("request")); err != nil || n != 7 {
		t.Fatalf("write: %d %v", n, err)
	}
	conn.readResults <- failoverRaceTestRead{data: []byte("response")}
	out := buf.NewSize(16)
	defer out.Release()
	if err := raced.ReadBuffer(out); err != nil || out.Len() != 8 {
		t.Fatalf("read: %d %v", out.Len(), err)
	}
	if got := confirmed.Load(); got != 1 {
		t.Fatalf("expected one confirmation, got %d", got)
	}
	conn.readResults <- failoverRaceTestRead{data: []byte("again")}
	buffer := buf.NewSize(16)
	defer buffer.Release()
	if err := raced.ReadBuffer(buffer); err != nil {
		t.Fatal(err)
	}
	if got := confirmed.Load(); got != 1 {
		t.Fatalf("duplicate confirmation: %d", got)
	}
}
