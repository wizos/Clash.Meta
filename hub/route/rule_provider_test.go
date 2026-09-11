package route

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/listener"
	LC "github.com/metacubex/mihomo/listener/config"
	R "github.com/metacubex/mihomo/rules"
	RP "github.com/metacubex/mihomo/rules/provider"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

type apiTestRuleProvider struct {
	name    string
	updates int
}

type apiTestRuleVehicle struct {
	reads int
}

func (v *apiTestRuleVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	v.reads++
	payload := []byte("192.0.2.0/24\n")
	return payload, utils.MakeHash(payload), nil
}

func (*apiTestRuleVehicle) Write([]byte) error  { return nil }
func (*apiTestRuleVehicle) Path() string        { return "" }
func (*apiTestRuleVehicle) Url() string         { return "test://unused" }
func (*apiTestRuleVehicle) Proxy() string       { return "" }
func (*apiTestRuleVehicle) Type() P.VehicleType { return P.HTTP }

func (p *apiTestRuleProvider) Name() string                            { return p.name }
func (*apiTestRuleProvider) VehicleType() P.VehicleType                { return P.HTTP }
func (*apiTestRuleProvider) Type() P.ProviderType                      { return P.Rule }
func (*apiTestRuleProvider) Initial() error                            { return nil }
func (p *apiTestRuleProvider) Update() error                           { p.updates++; return nil }
func (*apiTestRuleProvider) Behavior() P.RuleBehavior                  { return P.Domain }
func (*apiTestRuleProvider) Count() int                                { return 0 }
func (*apiTestRuleProvider) Match(*C.Metadata, C.RuleMatchHelper) bool { return false }
func (*apiTestRuleProvider) Strategy() any                             { return nil }
func (p *apiTestRuleProvider) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.name})
}

func TestRuleProviderAPIKeepsUnusedProviderAndAllowsUpdate(t *testing.T) {
	provider := &apiTestRuleProvider{name: "unused"}
	oldProviders := tunnel.RuleProviders()
	tunnel.UpdateRules(nil, nil, map[string]P.RuleProvider{provider.name: provider})
	t.Cleanup(func() { tunnel.UpdateRules(nil, nil, oldProviders) })
	router := ruleProviderRouter()

	getResponse := httptest.NewRecorder()
	router.ServeHTTP(getResponse, httptest.NewRequest(http.MethodGet, "/", nil))
	if getResponse.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getResponse.Code, http.StatusOK)
	}
	var body struct {
		Providers map[string]json.RawMessage `json:"providers"`
	}
	if err := json.Unmarshal(getResponse.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body.Providers[provider.name]; !ok {
		t.Fatal("unused provider was missing from API response")
	}

	updateResponse := httptest.NewRecorder()
	router.ServeHTTP(updateResponse, httptest.NewRequest(http.MethodPut, "/unused", nil))
	if updateResponse.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", updateResponse.Code, http.StatusNoContent)
	}
	if provider.updates != 1 {
		t.Fatalf("update calls = %d, want 1", provider.updates)
	}
}

func TestPatchConfigsInitializesNewTunRuleProviderBeforeRecreate(t *testing.T) {
	vehicle := &apiTestRuleVehicle{}
	RP.SetTunnel(tunnel.Tunnel)
	provider := RP.NewRuleSetProvider(
		"unused",
		P.IPCIDR,
		P.TextRule,
		0,
		vehicle,
		nil,
		nil,
		R.ParseRule,
	)
	if closer, ok := provider.(interface{ Close() error }); ok {
		t.Cleanup(func() { _ = closer.Close() })
	}
	oldProviders := tunnel.RuleProviders()
	oldTunConfig := listener.LastTunConf
	tunnel.UpdateRules(nil, nil, map[string]P.RuleProvider{"unused": provider})
	listener.LastTunConf = LC.Tun{}
	t.Cleanup(func() {
		tunnel.UpdateRules(nil, nil, oldProviders)
		listener.LastTunConf = oldTunConfig
	})

	if vehicle.reads != 0 {
		t.Fatalf("startup reads = %d, want 0", vehicle.reads)
	}
	patchTunConfig(t, `{"tun":{"enable":false,"route-address-set":["unused"]}}`)
	if vehicle.reads != 1 {
		t.Fatalf("reads after adding route set = %d, want 1", vehicle.reads)
	}
	if provider.Count() != 1 {
		t.Fatalf("provider count after PATCH = %d, want 1", provider.Count())
	}

	patchTunConfig(t, `{"tun":{"enable":false,"route-address-set":[]}}`)
	patchTunConfig(t, `{"tun":{"enable":false,"route-exclude-address-set":["unused"]}}`)
	patchTunConfig(t, `{"tun":{"enable":false,"route-exclude-address-set":["unused"]}}`)
	if vehicle.reads != 1 {
		t.Fatalf("reads after remove/re-add/reload = %d, want 1", vehicle.reads)
	}
}

func patchTunConfig(t *testing.T, payload string) {
	t.Helper()
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPatch, "/configs", strings.NewReader(payload))
	patchConfigs(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("PATCH status = %d, want %d", response.Code, http.StatusNoContent)
	}
}
