package common

import (
	"net/netip"
	"testing"

	"github.com/metacubex/mihomo/component/geodata"
	C "github.com/metacubex/mihomo/constant"
)

func TestGEOIPInvertMatchUsesCachedDestinationCodes(t *testing.T) {
	geodata.SetGeodataMode(false)
	rule := &GEOIP{country: "cn", adapter: "proxy", invert: true}

	matched, adapter := rule.Match(&C.Metadata{
		DstIP:    netip.MustParseAddr("8.8.8.8"),
		DstGeoIP: []string{"us"},
	}, C.RuleMatchHelper{})

	if !matched || adapter != "proxy" {
		t.Fatalf("expected non-CN GeoIP to match, got matched=%v adapter=%q", matched, adapter)
	}
}

func TestGEOIPInvertMatchSkipsUnknownDestinationCodes(t *testing.T) {
	geodata.SetGeodataMode(false)
	rule := &GEOIP{country: "cn", adapter: "proxy", invert: true}

	matched, _ := rule.Match(&C.Metadata{
		DstIP:    netip.MustParseAddr("203.0.113.1"),
		DstGeoIP: []string{},
	}, C.RuleMatchHelper{})

	if matched {
		t.Fatal("expected unknown GeoIP to skip inverted GeoIP match")
	}
}

func TestNewGEOIPInvertRejectsGeodataMode(t *testing.T) {
	geodata.SetGeodataMode(true)
	defer geodata.SetGeodataMode(false)

	if _, err := NewGEOIP("!CN", "proxy", false, false); err == nil {
		t.Fatal("expected inverted GeoIP to be rejected in geodata mode")
	}
}
