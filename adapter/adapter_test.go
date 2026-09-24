package adapter

import (
	"testing"
	"time"
)

func TestDelayMillisecondsSaturates(t *testing.T) {
	for _, test := range []struct {
		elapsed time.Duration
		want    uint16
	}{
		{0, 1},
		{500 * time.Microsecond, 1},
		{65534 * time.Millisecond, 65534},
		{65535 * time.Millisecond, 65535},
		{65536 * time.Millisecond, 65535},
		{130000 * time.Millisecond, 65535},
	} {
		if got := delayMilliseconds(test.elapsed); got != test.want {
			t.Errorf("delayMilliseconds(%s) = %d, want %d", test.elapsed, got, test.want)
		}
	}
}
