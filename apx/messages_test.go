package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocationChecksUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []int64
		wantErr bool
	}{
		{"ints", `{"locations":[1,2,3]}`, []int64{1, 2, 3}, false},
		{"floats", `{"locations":[1.0,2.0,3.0]}`, []int64{1, 2, 3}, false},
		{"mixed", `{"locations":[1,2.0,3]}`, []int64{1, 2, 3}, false},
		{"fractional", `{"locations":[1.5]}`, nil, true},
		{"empty", `{"locations":[]}`, []int64{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var msg LocationChecksMessage
			err := json.Unmarshal([]byte(tt.input), &msg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				require.Equal(t, tt.want, msg.Locations)
			}
		})
	}
}
