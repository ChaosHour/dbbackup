#!/bin/bash

echo "╔═══════════════════════════════════════════════════════════╗"
echo "║              TUI VALIDATION SUITE                         ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo ""

TUI_PATH="internal/tui"
CMD_PATH="cmd"

ISSUES=0

echo "--- 1. Goroutine Panic Recovery ---"
# Every goroutine should have defer recover
while IFS= read -r line; do
    file=$(echo "$line" | cut -d: -f1)
    lineno=$(echo "$line" | cut -d: -f2)
    
    # Check next 30 lines for defer recover
    context=$(sed -n "$lineno,$((lineno+30))p" "$file" 2>/dev/null)
    
    if ! echo "$context" | grep -q "defer.*recover"; then
        echo "⚠️  No panic recovery: $file:$lineno"
        ((ISSUES++))
    fi
done < <(grep -rn "go func" $TUI_PATH $CMD_PATH --include="*.go" 2>/dev/null)

GOROUTINE_ISSUES=$ISSUES
echo "Found $GOROUTINE_ISSUES goroutines without panic recovery"
echo ""

echo "--- 2. Program.Send() Safety ---"
SEND_ISSUES=0
while IFS= read -r line; do
    file=$(echo "$line" | cut -d: -f1)
    lineno=$(echo "$line" | cut -d: -f2)
    
    # Check if there's a nil check before Send
    context=$(sed -n "$((lineno-5)),$lineno p" "$file" 2>/dev/null)
    
    if ! echo "$context" | grep -qE "!= nil|if.*program"; then
        echo "⚠️  Unsafe Send (no nil check): $file:$lineno"
        ((SEND_ISSUES++))
    fi
done < <(grep -rn "\.Send(" $TUI_PATH --include="*.go" 2>/dev/null)
echo "Found $SEND_ISSUES unsafe Send() calls"
echo ""

echo "--- 3. Context Cancellation ---"
CTX_ISSUES=$(grep -rn "select {" $TUI_PATH --include="*.go" -A 20 2>/dev/null | \
    grep -B 5 -A 15 "case.*<-.*:" | \
    grep -v "ctx.Done()\|context.Done" | wc -l)
echo "Select statements without ctx.Done(): $CTX_ISSUES lines"
echo ""

echo "--- 4. Mutex Protection ---"
echo "Models with shared state (review for mutex):"
grep -rn "type.*Model.*struct" $TUI_PATH --include="*.go" 2>/dev/null | head -10
echo ""

echo "--- 5. Channel Operations ---"
UNBUFFERED=$(grep -rn "make(chan" $TUI_PATH $CMD_PATH --include="*.go" 2>/dev/null | grep -v ", [0-9]" | wc -l)
echo "Unbuffered channels (may block): $UNBUFFERED"
echo ""

echo "--- 6. tea.Cmd Safety ---"
NULL_CMDS=$(grep -rn "return.*nil$" $TUI_PATH --include="*.go" 2>/dev/null | grep "tea.Cmd\|Init\|Update" | wc -l)
echo "Functions returning nil Cmd: $NULL_CMDS (OK)"
echo ""

echo "--- 7. State Machine Completeness ---"
echo "Message types handled in Update():"
grep -rn "case.*Msg:" $TUI_PATH --include="*.go" 2>/dev/null | wc -l
echo ""

echo "═══════════════════════════════════════════════════════════"
TOTAL=$((GOROUTINE_ISSUES + SEND_ISSUES))
if [[ $TOTAL -eq 0 ]]; then
    echo "✅ TUI VALIDATION PASSED - No critical issues found"
else
    echo "⚠️  TUI VALIDATION: $TOTAL potential issues found"
fi
