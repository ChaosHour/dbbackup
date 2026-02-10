package tui

import (
	"testing"
)

func TestQuotePGIdent_Injection(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"mydb", `"mydb"`},
		{`my"db`, `"my""db"`},
		{"simple_name", `"simple_name"`},
		{"UPPER", `"UPPER"`},
		{`"; DROP DATABASE prod; --`, `"""; DROP DATABASE prod; --"`}, // " → "" inside outer quotes
	}

	for _, tt := range tests {
		result := quotePGIdent(tt.input)
		if result != tt.expected {
			t.Errorf("quotePGIdent(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestQuoteMySQLIdent_Injection(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"mydb", "`mydb`"},
		{"my`db", "`my``db`"},
		{"simple_name", "`simple_name`"},
		{"`; DROP DATABASE prod; --", "```; DROP DATABASE prod; --`"},
	}

	for _, tt := range tests {
		result := quoteMySQLIdent(tt.input)
		if result != tt.expected {
			t.Errorf("quoteMySQLIdent(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestValidateDBIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		maxLen  int
		wantErr bool
	}{
		{"valid", "mydb", 63, false},
		{"empty", "", 63, true},
		{"too long pg", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 63, true}, // 64 chars
		{"too long mysql", string(make([]byte, 65)), 64, true},
		{"null byte", "my\x00db", 63, true},
		{"with spaces", "my db", 63, false},     // spaces are valid in quoted identifiers
		{"with special", "my-db!@#", 63, false}, // valid when quoted
		{"unicode", "базаданных", 63, false},    // unicode is fine
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDBIdentifier(tt.input, tt.maxLen)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDBIdentifier(%q, %d) error = %v, wantErr %v", tt.input, tt.maxLen, err, tt.wantErr)
			}
		})
	}
}
