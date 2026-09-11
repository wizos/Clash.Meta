package provider

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	P "github.com/metacubex/mihomo/constant/provider"
)

type testProviderTunnel struct {
	callback *utils.Callback[P.RuleProvider]
}

func (*testProviderTunnel) Providers() map[string]P.ProxyProvider    { return nil }
func (*testProviderTunnel) RuleProviders() map[string]P.RuleProvider { return nil }
func (t *testProviderTunnel) RuleUpdateCallback() *utils.Callback[P.RuleProvider] {
	return t.callback
}

type testProviderVehicle struct {
	mu        sync.Mutex
	reads     int
	payloads  [][]byte
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (v *testProviderVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	v.mu.Lock()
	index := v.reads
	if index >= len(v.payloads) {
		index = len(v.payloads) - 1
	}
	v.reads++
	payload := v.payloads[index]
	started, release := v.started, v.release
	v.mu.Unlock()
	if started != nil {
		v.startOnce.Do(func() { close(started) })
		<-release
	}
	return payload, utils.MakeHash(payload), nil
}

func (*testProviderVehicle) Write([]byte) error  { return nil }
func (*testProviderVehicle) Path() string        { return "" }
func (*testProviderVehicle) Url() string         { return "test://provider" }
func (*testProviderVehicle) Proxy() string       { return "" }
func (*testProviderVehicle) Type() P.VehicleType { return P.HTTP }

func (v *testProviderVehicle) readCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.reads
}

func TestRuleSetProviderInitialConcurrentAndIdempotent(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	vehicle := &testProviderVehicle{
		payloads: [][]byte{[]byte("payload:\n  - example.com\n")},
		started:  started,
		release:  release,
	}
	provider := newTestRuleSetProvider(t, vehicle)

	const calls = 8
	errors := make(chan error, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- provider.Initial()
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Initial did not reach vehicle read")
	}
	close(release)
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := provider.Initial(); err != nil {
		t.Fatal(err)
	}
	if reads := vehicle.readCount(); reads != 1 {
		t.Fatalf("reads = %d, want 1", reads)
	}
	if provider.Count() != 1 {
		t.Fatalf("provider count = %d, want 1", provider.Count())
	}
}

func TestRuleSetProviderInitialCachesFailureAndManualUpdateRecovers(t *testing.T) {
	vehicle := &testProviderVehicle{payloads: [][]byte{
		[]byte("invalid"),
		[]byte("payload:\n  - example.com\n"),
	}}
	provider := newTestRuleSetProvider(t, vehicle)

	if err := provider.Initial(); err == nil {
		t.Fatal("first Initial succeeded, want parse error")
	}
	if err := provider.Initial(); err == nil {
		t.Fatal("second Initial succeeded, want cached parse error")
	}
	if reads := vehicle.readCount(); reads != 1 {
		t.Fatalf("reads after repeated Initial = %d, want 1", reads)
	}
	if err := provider.Update(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Initial(); err == nil {
		t.Fatal("Initial after Update succeeded, want cached parse error")
	}
	if reads := vehicle.readCount(); reads != 2 {
		t.Fatalf("reads = %d, want 2", reads)
	}
	if provider.Count() != 1 {
		t.Fatalf("provider count = %d, want 1", provider.Count())
	}
}

func newTestRuleSetProvider(t *testing.T, vehicle P.Vehicle) P.RuleProvider {
	t.Helper()
	oldTunnel := tunnel
	SetTunnel(&testProviderTunnel{callback: utils.NewCallback[P.RuleProvider]()})
	t.Cleanup(func() { tunnel = oldTunnel })
	provider := NewRuleSetProvider("test", P.Domain, P.YamlRule, 0, vehicle, nil, nil, nil)
	if closer, ok := provider.(interface{ Close() error }); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}
	return provider
}

var _ P.Tunnel = (*testProviderTunnel)(nil)
var _ P.Vehicle = (*testProviderVehicle)(nil)
