package config_test

import (
	"strings"
	"testing"

	"github.com/metacubex/mihomo/config"
	_ "github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/tunnel"
)

func TestRequiredRuleProvidersIncludesEveryConsumerRoot(t *testing.T) {
	parsed, err := config.Parse([]byte(`
rule-providers:
  main-root: {type: inline, behavior: domain, payload: [main.example]}
  logic-root: {type: inline, behavior: domain, payload: [logic.example]}
  sub-root: {type: inline, behavior: domain, payload: [sub.example]}
  dns-root: {type: inline, behavior: domain, payload: [dns.example]}
  proxy-dns-root: {type: inline, behavior: domain, payload: [proxy-dns.example]}
  fake-rule-root: {type: inline, behavior: domain, payload: [fake.example]}
  sniffer-force-root: {type: inline, behavior: domain, payload: [force.example]}
  sniffer-skip-root: {type: inline, behavior: domain, payload: [skip.example]}
  sniffer-src-root: {type: inline, behavior: ipcidr, payload: [10.0.0.0/8]}
  sniffer-dst-root: {type: inline, behavior: ipcidr, payload: [172.16.0.0/12]}
  tun-route-root: {type: inline, behavior: ipcidr, payload: [192.0.2.0/24]}
  tun-exclude-root: {type: inline, behavior: ipcidr, payload: [198.51.100.0/24]}
  custom-route-root: {type: inline, behavior: ipcidr, payload: [203.0.113.0/24]}
  custom-exclude-root: {type: inline, behavior: ipcidr, payload: [2001:db8::/32]}
  unused: {type: inline, behavior: domain, payload: [unused.example]}
rules:
  - RULE-SET,main-root,DIRECT
  - AND,((RULE-SET,logic-root),(DOMAIN,logic.example)),DIRECT
sub-rules:
  detached:
    - RULE-SET,sub-root,DIRECT
dns:
  enhanced-mode: fake-ip
  fake-ip-filter-mode: rule
  fake-ip-filter:
    - RULE-SET,fake-rule-root,real-ip
  nameserver-policy:
    "rule-set:dns-root": 114.114.114.114
  proxy-server-nameserver: [8.8.8.8]
  proxy-server-nameserver-policy:
    "rule-set:proxy-dns-root": 8.8.8.8
sniffer:
  force-domain: [rule-set:sniffer-force-root]
  skip-domain: [rule-set:sniffer-skip-root]
  skip-src-address: [rule-set:sniffer-src-root]
  skip-dst-address: [rule-set:sniffer-dst-root]
tun:
  route-address-set: [tun-route-root]
  route-exclude-address-set: [tun-exclude-root]
listeners:
  - name: custom-tun
    type: tun
    route-address-set: [custom-route-root]
    route-exclude-address-set: [custom-exclude-root]
`))
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"main-root", "logic-root", "sub-root", "dns-root", "proxy-dns-root",
		"fake-rule-root", "sniffer-force-root", "sniffer-skip-root",
		"sniffer-src-root", "sniffer-dst-root", "tun-route-root",
		"tun-exclude-root", "custom-route-root", "custom-exclude-root",
	}
	if len(parsed.RequiredRuleProviders) != len(want) {
		t.Fatalf("required providers = %v, want %v", parsed.RequiredRuleProviders, want)
	}
	for _, name := range want {
		if _, ok := parsed.RequiredRuleProviders[name]; !ok {
			t.Errorf("required providers does not contain %q", name)
		}
	}
	if _, ok := parsed.RequiredRuleProviders["unused"]; ok {
		t.Error("unused provider was marked required")
	}
	if _, ok := parsed.RuleProviders["unused"]; !ok {
		t.Error("unused provider descriptor was discarded")
	}
}

func TestRequiredRuleProvidersIncludesFakeIPDomainMatcher(t *testing.T) {
	parsed, err := config.Parse([]byte(`
rule-providers:
  fake-domain-root: {type: inline, behavior: domain, payload: [fake.example]}
  unused: {type: inline, behavior: domain, payload: [unused.example]}
dns:
  enhanced-mode: fake-ip
  fake-ip-filter: [.ordinary.example, rule-set:fake-domain-root]
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parsed.RequiredRuleProviders["fake-domain-root"]; !ok {
		t.Error("fake-ip domain matcher provider was not marked required")
	}
	if _, ok := parsed.RequiredRuleProviders["unused"]; ok {
		t.Error("unused provider was marked required")
	}
	if err := parsed.RuleProviders["fake-domain-root"].Initial(); err != nil {
		t.Fatal(err)
	}
	previousProviders, previousRules := tunnel.RuleProvidersSnapshot(), tunnel.Rules()
	tunnel.UpdateRules(parsed.Rules, parsed.SubRules, parsed.RuleProviders)
	t.Cleanup(func() { tunnel.UpdateRules(previousRules, nil, previousProviders) })
	for _, domain := range []string{"one.ordinary.example", "fake.example"} {
		if !parsed.DNS.FakeIPSkipper.ShouldSkipped(domain) {
			t.Errorf("fake-ip filter lost domain %q after combining a DomainSet and a provider", domain)
		}
	}
}

func TestRuleProviderValidationStillCoversUnusedDeclarationsAndSubRules(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "unused invalid declaration",
			config: `
rule-providers:
  unused: {type: inline, behavior: invalid, payload: [unused.example]}
`,
			errContains: "unsupported behavior type",
		},
		{
			name: "missing provider in detached sub-rule",
			config: `
sub-rules:
  detached:
    - RULE-SET,missing,DIRECT
`,
			errContains: "rule set [missing] not found",
		},
		{
			name: "circular sub-rules",
			config: `
sub-rules:
  first:
    - SUB-RULE,(NETWORK,TCP),second
  second:
    - SUB-RULE,(NETWORK,TCP),first
`,
			errContains: "circular references",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.Parse([]byte(test.config))
			if err == nil || !strings.Contains(err.Error(), test.errContains) {
				t.Fatalf("error = %v, want containing %q", err, test.errContains)
			}
		})
	}
}
