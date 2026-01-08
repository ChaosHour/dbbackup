#!/bin/bash
# Remove ALL emojis/unicode symbols from Go code and replace with ASCII
# Date: January 8, 2026
# Issue: 638 lines contain Unicode emojis causing display issues

set -euo pipefail

echo "[INFO] Starting emoji removal process..."
echo ""

# Find all Go files with emojis (expanded emoji list)
echo "[SEARCH] Finding affected files..."
FILES=$(find . -name "*.go" -type f -not -path "*/vendor/*" -not -path "*/.git/*" | xargs grep -l -P '[\x{1F000}-\x{1FFFF}]|[\x{2300}-\x{27BF}]|[\x{2600}-\x{26FF}]' 2>/dev/null || true)

if [ -z "$FILES" ]; then
    echo "[WARN] No files with emojis found!"
    exit 0
fi

FILECOUNT=$(echo "$FILES" | wc -l)
echo "[INFO] Found $FILECOUNT files containing emojis"
echo ""

# Count total emojis before
BEFORE=$(find . -name "*.go" -type f -not -path "*/vendor/*" | xargs grep -oP '[\x{1F000}-\x{1FFFF}]|[\x{2300}-\x{27BF}]|[\x{2600}-\x{26FF}]' 2>/dev/null | wc -l || echo "0")
echo "[INFO] Total emojis found: $BEFORE"
echo ""

# Create backup
BACKUP_DIR="backup_before_emoji_removal_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
echo "[INFO] Creating backup in $BACKUP_DIR..."
for file in $FILES; do
    mkdir -p "$BACKUP_DIR/$(dirname "$file")"
    cp "$file" "$BACKUP_DIR/$file"
done
echo "[OK] Backup created"
echo ""

# Process each file
echo "[INFO] Replacing emojis with ASCII equivalents..."
PROCESSED=0

for file in $FILES; do
    PROCESSED=$((PROCESSED + 1))
    echo "[$PROCESSED/$FILECOUNT] Processing: $file"
    
    # Create temp file
    TMPFILE="${file}.tmp"
    
    # Status indicators
    sed 's/✅/[OK]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/❌/[FAIL]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/✓/[+]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/✗/[-]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Warning symbols (⚠️ has variant selector, handle both)
    sed 's/⚠️/[WARN]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/⚠/[!]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Info/Data symbols
    sed 's/📊/[INFO]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📋/[LIST]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📁/[DIR]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📦/[PKG]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Target/Security
    sed 's/🎯/[TARGET]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🛡️/[SECURE]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🔒/[LOCK]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🔓/[UNLOCK]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Actions
    sed 's/🔍/[SEARCH]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/⏱️/[TIME]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Cloud operations (☁️ has variant selector, handle both)
    sed 's/☁️/[CLOUD]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/☁/[CLOUD]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📤/[UPLOAD]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📥/[DOWNLOAD]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🗑️/[DELETE]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Other
    sed 's/📈/[UP]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/📉/[DOWN]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    
    # Additional emojis found
    sed 's/⌨️/[KEY]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/⌨/[KEY]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🗄️/[DB]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/🗄/[DB]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/⚙️/[CONFIG]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/⚙/[CONFIG]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/✏️/[EDIT]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
    sed 's/✏/[EDIT]/g' "$file" > "$TMPFILE" && mv "$TMPFILE" "$file"
done

echo ""
echo "[OK] Replacement complete!"
echo ""

# Count remaining emojis
AFTER=$(find . -name "*.go" -type f -not -path "*/vendor/*" | xargs grep -oP '[\x{1F000}-\x{1FFFF}]|[\x{2300}-\x{27BF}]|[\x{2600}-\x{26FF}]' 2>/dev/null | wc -l || echo "0")

echo "[INFO] Emojis before: $BEFORE"
echo "[INFO] Emojis after: $AFTER"
echo "[INFO] Emojis removed: $((BEFORE - AFTER))"
echo ""

if [ "$AFTER" -gt 0 ]; then
    echo "[WARN] $AFTER emojis still remaining!"
    echo "[INFO] Listing remaining emojis:"
    find . -name "*.go" -type f -not -path "*/vendor/*" | xargs grep -nP '[\x{1F000}-\x{1FFFF}]|[\x{2300}-\x{27BF}]|[\x{2600}-\x{26FF}]' 2>/dev/null | head -20
else
    echo "[OK] All emojis successfully removed!"
fi

echo ""
echo "[INFO] Backup location: $BACKUP_DIR"
echo "[INFO] To restore: cp -r $BACKUP_DIR/* ."
echo ""
echo "[INFO] Next steps:"
echo "  1. Build: go build"
echo "  2. Test: go test ./..."
echo "  3. Manual testing: ./dbbackup status"
echo "  4. If OK, commit: git add . && git commit -m 'Replace emojis with ASCII'"
echo "  5. If broken, restore: cp -r $BACKUP_DIR/* ."
echo ""
echo "[OK] Emoji removal script completed!"
