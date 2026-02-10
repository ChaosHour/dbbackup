package database

import "strings"

// QuotePGIdentifier safely quotes a PostgreSQL identifier (table, database, role name)
// by wrapping in double-quotes and doubling any internal double-quotes.
//
//	QuotePGIdentifier(`my"db`) → `"my""db"`
func QuotePGIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteMySQLIdentifier safely quotes a MySQL/MariaDB identifier by wrapping
// in backticks and doubling any internal backticks.
//
//	QuoteMySQLIdentifier("my`db") → "`my``db`"
func QuoteMySQLIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// EscapePGLiteral safely escapes a PostgreSQL string literal value by doubling
// single-quotes. The result should be placed inside single-quotes in a SQL query.
//
//	"SELECT 1 FROM pg_database WHERE datname='" + EscapePGLiteral(name) + "'"
func EscapePGLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

// EscapeMySQLLiteral safely escapes a MySQL string literal value by escaping
// backslashes and single-quotes. The result should be placed inside single-quotes.
func EscapeMySQLLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", `\'`)
	return value
}
