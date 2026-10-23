package outboundgroup

import (
	"context"
	"net"
	"testing"

	"github.com/metacubex/mihomo/adapter"
	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type upstreamCountingProxy struct {
	C.Proxy
	calls int
	last  C.Conn
}

func (p *upstreamCountingProxy) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	p.calls++
	local, peer := net.Pipe()
	_ = peer.Close()
	p.last = outbound.NewConn(local, p.Proxy)
	return p.last, nil
}

func TestUpstreamGroupsIgnoreFailoverRace(t *testing.T) {
	for _, kind := range []string{"fallback", "url-test"} {
		for _, flag := range []string{"absent", "true", "false"} {
			t.Run(kind+"/"+flag, func(t *testing.T) {
				primary := &upstreamCountingProxy{Proxy: adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "primary"}))}
				backup := &upstreamCountingProxy{Proxy: adapter.NewProxy(outbound.NewDirectWithOption(outbound.DirectOption{Name: "backup"}))}
				proxies := map[string]C.Proxy{"primary": primary, "backup": backup, "COMPATIBLE": primary}
				mapping := map[string]any{"name": "group", "type": kind, "proxies": []string{"primary", "backup"}, "lazy": true}
				if flag != "absent" {
					mapping["failover-race"] = flag == "true"
				}
				group, err := ParseProxyGroup(mapping, proxies, map[string]P.ProxyProvider{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				conn, err := group.DialContext(context.Background(), &C.Metadata{NetWork: C.TCP, Host: "example.test", DstPort: 443})
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if conn != primary.last || primary.calls != 1 || backup.calls != 0 {
					t.Fatalf("upstream single-path behavior changed: conn=%T primary=%d backup=%d", conn, primary.calls, backup.calls)
				}
			})
		}
	}
}
