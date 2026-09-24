package kafka

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// A failing registry is logged by the relay, and a log line is the one place a connection
// string reliably outlives the process that read it.
func TestAddressNamesTheRegistryWithoutItsCredentials(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    string
		secret  string
		noParse bool
	}{
		{
			name: "the lab registry, nothing to hide",
			args: "http://localhost:8080/apis/ccompat/v7",
			want: "http://localhost:8080/apis/ccompat/v7",
		},
		{
			name:   "basic auth in the url",
			args:   "https://relay:s3cret@registry.internal:8081/apis/ccompat/v7",
			want:   "https://registry.internal:8081/apis/ccompat/v7",
			secret: "s3cret",
		},
		{
			name:   "a token smuggled in the query",
			args:   "https://registry.internal:8081/apis?access_token=abc123",
			want:   "https://registry.internal:8081/apis",
			secret: "abc123",
		},
		{
			name:    "not a url at all",
			args:    "://registry",
			want:    "the configured schema registry",
			noParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := address(tt.args)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("address mismatch (-want +got):\n%s", diff)
			}
			if tt.secret != "" && strings.Contains(got, tt.secret) {
				t.Errorf("address %q still carries the credential from the url", got)
			}
		})
	}
}
