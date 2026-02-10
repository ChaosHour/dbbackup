package database

import "testing"

func TestQuotePGIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mydb", `"mydb"`},
		{`my"db`, `"my""db"`},
		{`a""b`, `"a""""b"`},
		{"", `""`},
		{"test-db", `"test-db"`},
		{"Test DB", `"Test DB"`},
		// Input: "; DROP DATABASE foo; -- (contains one " that gets doubled)
		{`"; DROP DATABASE foo; --`, `"""; DROP DATABASE foo; --"`},
	}
	for _, tt := range tests {
		got := QuotePGIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuotePGIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestQuoteMySQLIdentifier(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mydb", "`mydb`"},
		{"my`db", "`my``db`"},
		{"a``b", "`a````b`"},
		{"", "``"},
		{"test-db", "`test-db`"},
	}
	for _, tt := range tests {
		got := QuoteMySQLIdentifier(tt.input)
		if got != tt.want {
			t.Errorf("QuoteMySQLIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEscapePGLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mydb", "mydb"},
		{"my'db", "my''db"},
		{"O'Brien", "O''Brien"},
		{"a''b", "a''''b"},
		{"'; DROP TABLE foo; --", "''; DROP TABLE foo; --"},
	}
	for _, tt := range tests {
		got := EscapePGLiteral(tt.input)
		if got != tt.want {
			t.Errorf("EscapePGLiteral(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEscapeMySQLLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"mydb", "mydb"},
		{"my'db", `my\'db`},
		{`my\db`, `my\\db`},
		{`'; DROP TABLE foo; --`, `\'; DROP TABLE foo; --`},
	}
	for _, tt := range tests {
		got := EscapeMySQLLiteral(tt.input)
		if got != tt.want {
			t.Errorf("EscapeMySQLLiteral(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
