package config

import (
	"reflect"
	"testing"
)

func TestUpstreamConfigIgnoresRaceExtensionKeys(t *testing.T) {
	baseline, err := UnmarshalRawConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{
		"failover-race: true\nfailover-race-delay: 37\n",
		"failover-race: false\nfailover-race-delay: disabled\n",
	} {
		parsed, err := UnmarshalRawConfig([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(parsed, baseline) {
			t.Fatal("unsupported race extension changed Go configuration")
		}
	}
}
