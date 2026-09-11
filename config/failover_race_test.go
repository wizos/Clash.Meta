package config

import "testing"

func TestFailoverRaceDelayDefaultAndOverride(t *testing.T) {
	defaultConfig, err := UnmarshalRawConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaultConfig.FailoverRaceDelay != 100 {
		t.Fatalf("default failover-race-delay=%d", defaultConfig.FailoverRaceDelay)
	}

	overrideConfig, err := UnmarshalRawConfig([]byte("failover-race-delay: 37\n"))
	if err != nil {
		t.Fatal(err)
	}
	if overrideConfig.FailoverRaceDelay != 37 {
		t.Fatalf("custom failover-race-delay=%d", overrideConfig.FailoverRaceDelay)
	}
	general, err := parseGeneral(overrideConfig)
	if err != nil {
		t.Fatal(err)
	}
	if general.FailoverRaceDelay != 37 {
		t.Fatalf("parsed failover-race-delay=%d", general.FailoverRaceDelay)
	}
}
