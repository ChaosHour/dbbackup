#!/bin/bash
# Coverage analysis script for dbbackup
set -e

echo "🧪 Running comprehensive coverage analysis..."
echo ""

# Run tests with coverage
go test -coverprofile=coverage.out -covermode=atomic ./... 2>&1 | tee test-output.txt

echo ""
echo "📊 Coverage by Package:"
echo "========================"
go tool cover -func=coverage.out | grep -E "^dbbackup" | awk '{
    pkg = $1
    gsub(/:[0-9]+:/, "", pkg)
    gsub(/dbbackup\//, "", pkg)
    cov = $NF
    gsub(/%/, "", cov)
    if (cov + 0 < 50) {
        status = "❌"
    } else if (cov + 0 < 80) {
        status = "⚠️"
    } else {
        status = "✅"
    }
    printf "%s %-50s %s\n", status, pkg, $NF
}' | sort -t'%' -k2 -n | uniq

echo ""
echo "📈 Total Coverage:"
go tool cover -func=coverage.out | grep "total:"

echo ""
echo "📄 HTML report generated: coverage.html"
go tool cover -html=coverage.out -o coverage.html

echo ""
echo "🎯 Packages with 0% coverage:"
go tool cover -func=coverage.out | grep "0.0%" | cut -d: -f1 | sort -u | head -20
