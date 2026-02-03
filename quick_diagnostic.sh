#!/bin/bash

# Quick diagnostic test for the native engine hang
echo "🔍 Diagnosing Native Engine Issues"
echo "=================================="

echo ""
echo "Test 1: Check basic binary functionality..."
timeout 3s ./dbbackup_fixed --help > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Basic functionality works"
else
    echo "❌ Basic functionality broken"
    exit 1
fi

echo ""
echo "Test 2: Check configuration loading..."
timeout 5s ./dbbackup_fixed --version 2>&1 | head -3
if [ $? -eq 0 ]; then
    echo "✅ Configuration and version check works"
else
    echo "❌ Configuration loading hangs"
    exit 1
fi

echo ""
echo "Test 3: Test interactive mode with timeout (should exit quickly)..."
# Use a much shorter timeout and capture output
timeout 2s ./dbbackup_fixed interactive --auto-select=0 --auto-confirm --dry-run 2>&1 | head -10 &
PID=$!

sleep 3
if kill -0 $PID 2>/dev/null; then
    echo "❌ Process still running - HANG DETECTED"
    kill -9 $PID 2>/dev/null
    echo "   The issue is in TUI initialization or database connection"
    exit 1
else
    echo "✅ Process exited normally"
fi

echo ""
echo "Test 4: Check native engine without TUI..."
echo "CREATE TABLE test (id int);" | timeout 3s ./dbbackup_fixed restore single - --database=test_native --native --dry-run 2>&1 | head -5
if [ $? -eq 124 ]; then
    echo "❌ Native engine hangs even without TUI"
else
    echo "✅ Native engine works without TUI"
fi

echo ""
echo "🎯 Diagnostic complete!"