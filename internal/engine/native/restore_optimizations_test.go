package native

import (
	"testing"
)

func TestExtractMySQLTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "backtick quoted",
			input:    "CREATE TABLE `users` (\n  `id` int ...",
			expected: "`users`",
		},
		{
			name:     "unquoted",
			input:    "CREATE TABLE users (",
			expected: "users",
		},
		{
			name:     "schema.table backtick",
			input:    "CREATE TABLE `mydb`.`orders` (",
			expected: "`mydb`.`orders`",
		},
		{
			name:     "IF NOT EXISTS",
			input:    "CREATE TABLE IF NOT EXISTS `products` (",
			expected: "`products`",
		},
		{
			name:     "schema.table unquoted",
			input:    "CREATE TABLE mydb.orders (",
			expected: "mydb.orders",
		},
		{
			name:     "double quoted",
			input:    `CREATE TABLE "users" (`,
			expected: `"users"`,
		},
		{
			name:     "with newline after name",
			input:    "CREATE TABLE `big_data`\n  ( id INT",
			expected: "`big_data`",
		},
		{
			name:     "not a CREATE TABLE",
			input:    "INSERT INTO users VALUES (1)",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractMySQLTableName(tt.input)
			if got != tt.expected {
				t.Errorf("extractMySQLTableName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseInsertValues(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int // number of rows
	}{
		{
			name:     "single row",
			input:    "(1,'hello','world');",
			expected: 1,
		},
		{
			name:     "multiple rows",
			input:    "(1,'a','b'),(2,'c','d'),(3,'e','f');",
			expected: 3,
		},
		{
			name:     "with NULL",
			input:    "(1,NULL,'test');",
			expected: 1,
		},
		{
			name:     "escaped quotes",
			input:    "(1,'it\\'s','ok');",
			expected: 1,
		},
		{
			name:     "numeric only",
			input:    "(1,2,3),(4,5,6);",
			expected: 2,
		},
		{
			name:     "empty values",
			input:    "",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := parseInsertValues(tt.input)
			if len(rows) != tt.expected {
				t.Errorf("parseInsertValues(%q): got %d rows, want %d", tt.input, len(rows), tt.expected)
			}
		})
	}
}

func TestParseInsertValuesContent(t *testing.T) {
	rows := parseInsertValues("(1,'hello',NULL,42.5);")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if len(row) != 4 {
		t.Fatalf("expected 4 columns, got %d: %v", len(row), row)
	}

	expected := []string{"1", "hello", "\\N", "42.5"}
	for i, exp := range expected {
		if row[i] != exp {
			t.Errorf("column %d: got %q, want %q", i, row[i], exp)
		}
	}
}

func TestConvertRowToTSV(t *testing.T) {
	tests := []struct {
		name     string
		row      []string
		expected string
	}{
		{
			name:     "simple",
			row:      []string{"1", "hello", "world"},
			expected: "1\thello\tworld",
		},
		{
			name:     "with NULL",
			row:      []string{"1", "\\N", "test"},
			expected: "1\t\\N\ttest",
		},
		{
			name:     "with tab in data",
			row:      []string{"1", "hello\tworld"},
			expected: "1\thello\\tworld",
		},
		{
			name:     "with newline in data",
			row:      []string{"1", "line1\nline2"},
			expected: "1\tline1\\nline2",
		},
		{
			name:     "with backslash",
			row:      []string{"path", "C:\\Users"},
			expected: "path\tC:\\\\Users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertRowToTSV(tt.row)
			if got != tt.expected {
				t.Errorf("convertRowToTSV(%v) = %q, want %q", tt.row, got, tt.expected)
			}
		})
	}
}

func TestSanitizeFileName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"`users`", "users"},
		{"mydb.orders", "mydb_orders"},
		{"path/table", "path_table"},
		{"normal_table", "normal_table"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeFileName(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeFileName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestDetectBinaryHeader(t *testing.T) {
	// Valid PostgreSQL binary COPY header
	validHeader := []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0x00}
	if !DetectBinaryHeader(validHeader) {
		t.Error("expected valid binary header to be detected")
	}

	// Extended header (with trailing data)
	extended := append(validHeader, 0x00, 0x00, 0x00, 0x00)
	if !DetectBinaryHeader(extended) {
		t.Error("expected extended binary header to be detected")
	}

	// Too short
	if DetectBinaryHeader([]byte("PGCOPY")) {
		t.Error("expected too-short data to not be detected as binary")
	}

	// Wrong data
	if DetectBinaryHeader([]byte("COPY users FR")) {
		t.Error("expected text COPY to not be detected as binary")
	}

	// Empty
	if DetectBinaryHeader(nil) {
		t.Error("expected nil to not be detected as binary")
	}
}
