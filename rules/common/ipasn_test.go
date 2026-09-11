package common

import (
	"net/netip"
	"testing"

	C "github.com/metacubex/mihomo/constant"
)

func TestASNMatchUsesCachedMetadata(t *testing.T) {
	tests := []struct {
		name             string
		metadata         C.Metadata
		asn              string
		isSourceIP       bool
		noResolveIP      bool
		wantMatch        bool
		wantAdapter      string
		wantResolveCalls int
	}{
		{
			name: "destination match",
			metadata: C.Metadata{
				DstIP:    netip.MustParseAddr("1.1.1.1"),
				DstIPASN: "13335 CLOUDFLARENET",
			},
			asn:              "13335",
			wantMatch:        true,
			wantAdapter:      "DIRECT",
			wantResolveCalls: 1,
		},
		{
			name: "destination mismatch without resolve",
			metadata: C.Metadata{
				DstIP:    netip.MustParseAddr("1.1.1.1"),
				DstIPASN: "13335 CLOUDFLARENET",
			},
			asn:         "64500",
			noResolveIP: true,
			wantAdapter: "DIRECT",
		},
		{
			name: "source match",
			metadata: C.Metadata{
				SrcIP:    netip.MustParseAddr("192.0.2.1"),
				SrcIPASN: "64500 Example",
			},
			asn:         "64500",
			isSourceIP:  true,
			wantMatch:   true,
			wantAdapter: "DIRECT",
		},
		{
			name: "cached unknown result",
			metadata: C.Metadata{
				DstIP:    netip.MustParseAddr("192.0.2.1"),
				DstIPASN: " ",
			},
			asn:              "64500",
			wantAdapter:      "DIRECT",
			wantResolveCalls: 1,
		},
		{
			name: "invalid destination remains unmatched",
			metadata: C.Metadata{
				DstIPASN: "13335 CLOUDFLARENET",
			},
			asn:              "13335",
			wantResolveCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &ASN{
				asn:         tt.asn,
				adapter:     "DIRECT",
				noResolveIP: tt.noResolveIP,
				isSourceIP:  tt.isSourceIP,
			}
			resolveCalls := 0
			matched, adapter := rule.Match(&tt.metadata, C.RuleMatchHelper{
				ResolveIP: func() {
					resolveCalls++
				},
			})

			if matched != tt.wantMatch {
				t.Fatalf("Match() matched = %v, want %v", matched, tt.wantMatch)
			}
			if adapter != tt.wantAdapter {
				t.Fatalf("Match() adapter = %q, want %q", adapter, tt.wantAdapter)
			}
			if resolveCalls != tt.wantResolveCalls {
				t.Fatalf("Match() ResolveIP calls = %d, want %d", resolveCalls, tt.wantResolveCalls)
			}
		})
	}
}

func TestASNMatchUsesCacheAfterSwapSrcDst(t *testing.T) {
	metadata := C.Metadata{
		SrcIP:    netip.MustParseAddr("192.0.2.1"),
		DstIP:    netip.MustParseAddr("198.51.100.1"),
		SrcIPASN: "64500 Source",
		DstIPASN: "64501 Destination",
	}
	metadata.SwapSrcDst()

	sourceRule := &ASN{asn: "64501", adapter: "DIRECT", isSourceIP: true}
	matched, adapter := sourceRule.Match(&metadata, C.RuleMatchHelper{})
	if !matched || adapter != "DIRECT" {
		t.Fatalf("source Match() = (%v, %q), want (true, %q)", matched, adapter, "DIRECT")
	}

	destinationRule := &ASN{asn: "64500", adapter: "DIRECT", noResolveIP: true}
	matched, adapter = destinationRule.Match(&metadata, C.RuleMatchHelper{})
	if !matched || adapter != "DIRECT" {
		t.Fatalf("destination Match() = (%v, %q), want (true, %q)", matched, adapter, "DIRECT")
	}
}
