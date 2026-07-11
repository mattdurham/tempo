package postgres

import "testing"

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "url dsn with password",
			dsn:  "postgres://myuser:supersecret@dbhost:5432/mydb?sslmode=disable",
			want: "postgres://myuser:%2A%2A%2A@dbhost:5432/mydb?sslmode=disable",
		},
		{
			name: "url dsn without password",
			dsn:  "postgres://myuser@dbhost:5432/mydb",
			want: "postgres://myuser@dbhost:5432/mydb",
		},
		{
			name: "keyword value dsn with password",
			dsn:  "host=dbhost port=5432 user=myuser password=supersecret dbname=mydb",
			want: "host=dbhost port=5432 user=myuser password=*** dbname=mydb",
		},
		{
			name: "keyword value dsn with quoted password",
			dsn:  "host=dbhost password='super secret' dbname=mydb",
			want: "host=dbhost password=*** dbname=mydb",
		},
		{
			name: "no password anywhere",
			dsn:  "host=dbhost dbname=mydb",
			want: "host=dbhost dbname=mydb",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactDSN(tt.dsn)
			if got != tt.want {
				t.Errorf("RedactDSN(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
			if got == tt.dsn && tt.dsn != tt.want {
				t.Errorf("RedactDSN(%q) returned the input unchanged -- password not redacted", tt.dsn)
			}
		})
	}
}

func TestConfig_ApplyDefaults(t *testing.T) {
	c := &Config{}
	c.applyDefaults()
	if c.MaxConns != 4 {
		t.Errorf("MaxConns default = %d, want 4", c.MaxConns)
	}
	if c.ConnectTimeout.Seconds() != 5 {
		t.Errorf("ConnectTimeout default = %v, want 5s", c.ConnectTimeout)
	}
}

func TestConfig_ApplyDefaults_PreservesExplicitValues(t *testing.T) {
	c := &Config{MaxConns: 10, ConnectTimeout: 30}
	c.applyDefaults()
	if c.MaxConns != 10 {
		t.Errorf("MaxConns = %d, want unchanged 10", c.MaxConns)
	}
	if c.ConnectTimeout != 30 {
		t.Errorf("ConnectTimeout = %v, want unchanged 30ns", c.ConnectTimeout)
	}
}
