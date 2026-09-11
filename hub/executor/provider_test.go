package executor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type testRuleProvider struct {
	name        string
	vehicle     P.VehicleType
	initial     chan struct{}
	initialOnce sync.Once
	initials    atomic.Int32
	initialErr  error
	updates     atomic.Int32
}

func (p *testRuleProvider) Name() string                            { return p.name }
func (p *testRuleProvider) VehicleType() P.VehicleType              { return p.vehicle }
func (*testRuleProvider) Type() P.ProviderType                      { return P.Rule }
func (*testRuleProvider) Behavior() P.RuleBehavior                  { return P.Domain }
func (*testRuleProvider) Count() int                                { return 0 }
func (*testRuleProvider) Match(*C.Metadata, C.RuleMatchHelper) bool { return false }
func (*testRuleProvider) Strategy() any                             { return nil }

func (p *testRuleProvider) Initial() error {
	p.initials.Add(1)
	p.initialOnce.Do(func() { close(p.initial) })
	return p.initialErr
}

func (p *testRuleProvider) Update() error {
	p.updates.Add(1)
	return nil
}

func TestLoadProviderInitializesRequiredAndInlineOnly(t *testing.T) {
	used := &testRuleProvider{name: "used", vehicle: P.HTTP, initial: make(chan struct{})}
	unused := &testRuleProvider{
		name:       "unused",
		vehicle:    P.HTTP,
		initial:    make(chan struct{}),
		initialErr: errors.New("deferred payload or download error"),
	}
	inline := &testRuleProvider{name: "inline", vehicle: P.Inline, initial: make(chan struct{})}
	providers := map[string]P.RuleProvider{
		used.name:   used,
		unused.name: unused,
		inline.name: inline,
	}

	loadProvider(providers, map[string]struct{}{used.name: {}})
	waitForInitial(t, used.initial)
	waitForInitial(t, inline.initial)
	select {
	case <-unused.initial:
		t.Fatal("unused external provider was initialized")
	case <-time.After(20 * time.Millisecond):
	}

	if used.initials.Load() != 1 || inline.initials.Load() != 1 {
		t.Fatalf("initial counts: used=%d inline=%d", used.initials.Load(), inline.initials.Load())
	}
	if err := unused.Update(); err != nil {
		t.Fatal(err)
	}
	if unused.updates.Load() != 1 {
		t.Fatal("unused provider did not support manual update")
	}
}

func waitForInitial(t *testing.T, initialized <-chan struct{}) {
	t.Helper()
	select {
	case <-initialized:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for provider initialization")
	}
}
