package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type Fallback struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	selected       string
	expectedStatus string
	failoverRace   bool
	failoverDelay  time.Duration
	raceNode       C.Proxy
	selectionMu    sync.Mutex
	selectionEpoch atomic.Uint64
}

func (f *Fallback) Now() string {
	proxy := f.currentProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if !f.failoverRace {
		proxy := f.findAliveProxy(true)
		c, err := proxy.DialContext(ctx, metadata)
		if err == nil {
			c.AppendToChains(f)
		} else {
			f.onDialFailed(proxy.Type(), err, f.healthCheck)
		}

		if N.NeedHandshake(c) {
			c = callback.NewFirstWriteCallBackConn(c, func(err error) {
				if err == nil {
					f.onDialSuccess()
				} else {
					f.onDialFailed(proxy.Type(), err, f.healthCheck)
				}
			})
		}

		return c, err
	}

	if !failoverRaceOwned(ctx) {
		if epoch, automatic := f.failoverRaceSnapshot(); automatic {
			primary, challenger := f.failoverRaceCandidates(true, metadata)
			if challenger != nil {
				return dialFirstIOFailover(ctx, metadata, primary, challenger, f.failoverDelay, f.dialRaceProxy, func() {
					f.commitFailoverRace(epoch, challenger)
				})
			}
			return f.dialRaceProxy(ctx, primary, metadata)
		}
	}

	return f.dialRaceProxy(ctx, f.findFailoverRaceProxy(true), metadata)
}

func (f *Fallback) dialRaceProxy(ctx context.Context, proxy C.Proxy, metadata *C.Metadata) (c C.Conn, err error) {
	c, err = proxy.DialContext(ctx, metadata)
	if err == nil {
		c.AppendToChains(f)
	} else {
		f.onDialFailed(proxy.Type(), err, f.healthCheck)
	}
	if N.NeedHandshake(c) {
		c = callback.NewFirstWriteCallBackConn(c, func(err error) {
			if err == nil {
				f.onDialSuccess()
			} else {
				f.onDialFailed(proxy.Type(), err, f.healthCheck)
			}
		})
	}
	return c, err
}

func (f *Fallback) failoverRaceSnapshot() (uint64, bool) {
	epoch := f.selectionEpoch.Load()
	f.selectionMu.Lock()
	automatic := f.selected == ""
	f.selectionMu.Unlock()
	return epoch, automatic
}

func (f *Fallback) failoverRaceCandidates(touch bool, metadata *C.Metadata) (C.Proxy, C.Proxy) {
	primary := f.findFailoverRaceProxy(touch)
	proxies := f.GetProxies(false)
	primaryIndex := 0
	for index, proxy := range proxies {
		if proxy.Name() == primary.Name() {
			primaryIndex = index
			break
		}
	}
	primaryLeaf := failoverRaceLeaf(primary, metadata)
	for offset := 1; offset < len(proxies); offset++ {
		proxy := proxies[(primaryIndex+offset)%len(proxies)]
		if proxy.AliveForTestUrl(f.testUrl) && failoverRaceLeaf(proxy, metadata) != primaryLeaf {
			return primary, proxy
		}
	}
	return primary, nil
}

func (f *Fallback) commitFailoverRace(epoch uint64, challenger C.Proxy) {
	f.selectionMu.Lock()
	defer f.selectionMu.Unlock()
	if f.selectionEpoch.Load() != epoch || f.selected != "" {
		return
	}
	f.raceNode = challenger
	f.selectionEpoch.Add(1)
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := f.currentProxy(true)
	pc, err := proxy.ListenPacketContext(ctx, metadata)
	if err == nil {
		pc.AppendToChains(f)
	}

	return pc, err
}

// SupportUDP implements C.ProxyAdapter
func (f *Fallback) SupportUDP() bool {
	if f.disableUDP {
		return false
	}

	proxy := f.currentProxy(false)
	return proxy.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fallback) IsL3Protocol(metadata *C.Metadata) bool {
	return f.currentProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (f *Fallback) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range f.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	f.selectionMu.Lock()
	selected := f.selected
	f.selectionMu.Unlock()
	return json.Marshal(map[string]any{
		"type":           f.Type().String(),
		"now":            f.Now(),
		"all":            all,
		"testUrl":        f.testUrl,
		"expectedStatus": f.expectedStatus,
		"fixed":          selected,
		"hidden":         f.Hidden(),
		"icon":           f.Icon(),
		"emptyFallback":  f.EmptyFallback().Name(),
	})
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.currentProxy(touch)
	return proxy
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	proxies := f.GetProxies(touch)
	for _, proxy := range proxies {
		if len(f.selected) == 0 {
			if proxy.AliveForTestUrl(f.testUrl) {
				return proxy
			}
		} else {
			if proxy.Name() == f.selected {
				if proxy.AliveForTestUrl(f.testUrl) {
					return proxy
				} else {
					f.selected = ""
				}
			}
		}
	}

	return proxies[0]
}

func (f *Fallback) currentProxy(touch bool) C.Proxy {
	if !f.failoverRace {
		return f.findAliveProxy(touch)
	}
	return f.findFailoverRaceProxy(touch)
}

func (f *Fallback) findFailoverRaceProxy(touch bool) C.Proxy {
	proxies := f.GetProxies(touch)
	f.selectionMu.Lock()
	defer f.selectionMu.Unlock()
	if f.selected == "" && f.raceNode != nil {
		for _, proxy := range proxies {
			if proxy.Name() == f.raceNode.Name() && proxy.AliveForTestUrl(f.testUrl) {
				f.raceNode = proxy
				return proxy
			}
		}
		f.raceNode = nil
	}
	for _, proxy := range proxies {
		if len(f.selected) == 0 {
			if proxy.AliveForTestUrl(f.testUrl) {
				return proxy
			}
		} else {
			if proxy.Name() == f.selected {
				if proxy.AliveForTestUrl(f.testUrl) {
					return proxy
				} else {
					f.selected = ""
					f.raceNode = nil
					f.selectionEpoch.Add(1)
				}
			}
		}
	}

	return proxies[0]
}

func (f *Fallback) Set(name string) error {
	var p C.Proxy
	for _, proxy := range f.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	f.ForceSet(name)
	if !p.AliveForTestUrl(f.testUrl) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
		defer cancel()
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
		_, _ = p.URLTest(ctx, f.testUrl, expectedStatus)
	}

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.selectionEpoch.Add(1)
	f.selectionMu.Lock()
	f.selected = name
	f.raceNode = nil
	f.selectionMu.Unlock()
}

func (f *Fallback) Providers() []P.ProxyProvider {
	return f.providers
}

func (f *Fallback) Proxies() []C.Proxy {
	return f.GetProxies(false)
}

func NewFallback(option *GroupCommonOption, emptyFallback C.Proxy, providers []P.ProxyProvider) *Fallback {
	return &Fallback{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Fallback,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		failoverRace:   option.FailoverRace,
		failoverDelay:  option.FailoverRaceDelay,
	}
}
