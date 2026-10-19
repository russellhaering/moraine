package objstore

import "testing"

func TestRangeHeader(t *testing.T) {
	cases := []struct {
		offset, length int64
		want           string
		wantErr        bool
	}{
		{0, 10, "bytes=0-9", false},
		{100, 48, "bytes=100-147", false},
		{-48, 48, "bytes=-48", false},
		{-48, 100, "bytes=-48", false},
		{-48, 10, "", true},
		{0, 0, "", true},
	}
	for _, c := range cases {
		got, err := rangeHeader(c.offset, c.length)
		if (err != nil) != c.wantErr || got != c.want {
			t.Errorf("rangeHeader(%d, %d) = %q, %v; want %q, err=%v", c.offset, c.length, got, err, c.want, c.wantErr)
		}
	}
}
