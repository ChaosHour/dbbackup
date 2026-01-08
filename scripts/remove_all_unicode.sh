#!/bin/bash
# COMPLETE emoji/Unicode removal - Replace ALL non-ASCII with ASCII equivalents
# Date: January 8, 2026

set -euo pipefail

echo "[INFO] Starting COMPLETE Unicode->ASCII replacement..."
echo ""

# Create backup
BACKUP_DIR="backup_unicode_removal_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"
echo "[INFO] Creating backup in $BACKUP_DIR..."
find . -name "*.go" -type f -not -path "*/vendor/*" -not -path "*/.git/*" -exec bash -c 'mkdir -p "$1/$(dirname "$2")" && cp "$2" "$1/$2"' -- "$BACKUP_DIR" {} \;
echo "[OK] Backup created"
echo ""

# Find all affected files
echo "[SEARCH] Finding files with Unicode..."
FILES=$(find . -name "*.go" -type f -not -path "*/vendor/*" -not -path "*/.git/*")

PROCESSED=0
TOTAL=$(echo "$FILES" | wc -l)

for file in $FILES; do
    PROCESSED=$((PROCESSED + 1))
   
    if ! grep -qP '[\x{80}-\x{FFFF}]' "$file" 2>/dev/null; then
        continue
    fi
    
    echo "[$PROCESSED/$TOTAL] Processing: $file"
    
    # Create temp file for atomic replacements
    TMPFILE="${file}.tmp"
    cp "$file" "$TMPFILE"
    
    # Box drawing / decorative (used in TUI borders)
    sed -i 's/─/-/g' "$TMPFILE"
    sed -i 's/━/-/g' "$TMPFILE"
    sed -i 's/│/|/g' "$TMPFILE"
    sed -i 's/║/|/g' "$TMPFILE"
    sed -i 's/├/+/g' "$TMPFILE"
    sed -i 's/└/+/g' "$TMPFILE"
    sed -i 's/╔/+/g' "$TMPFILE"
    sed -i 's/╗/+/g' "$TMPFILE"
    sed -i 's/╚/+/g' "$TMPFILE"
    sed -i 's/╝/+/g' "$TMPFILE"
    sed -i 's/╠/+/g' "$TMPFILE"
    sed -i 's/╣/+/g' "$TMPFILE"
    sed -i 's/═/=/g' "$TMPFILE"
    
    # Status symbols
    sed -i 's/✅/[OK]/g' "$TMPFILE"
    sed -i 's/❌/[FAIL]/g' "$TMPFILE"
    sed -i 's/✓/[+]/g' "$TMPFILE"
    sed -i 's/✗/[-]/g' "$TMPFILE"
    sed -i 's/⚠️/[WARN]/g' "$TMPFILE"
    sed -i 's/⚠/[!]/g' "$TMPFILE"
    sed -i 's/❓/[?]/g' "$TMPFILE"
    
    # Arrows
    sed -i 's/←/</g' "$TMPFILE"
    sed -i 's/→/>/g' "$TMPFILE"
    sed -i 's/↑/^/g' "$TMPFILE"
    sed -i 's/↓/v/g' "$TMPFILE"
    sed -i 's/▲/^/g' "$TMPFILE"
    sed -i 's/▼/v/g' "$TMPFILE"
    sed -i 's/▶/>/g' "$TMPFILE"
    
    # Shapes
    sed -i 's/●/*\*/g' "$TMPFILE"
    sed -i 's/○/o/g' "$TMPFILE"
    sed -i 's/⚪/o/g' "$TMPFILE"
    sed -i 's/•/-/g' "$TMPFILE"
    sed -i 's/█/#/g' "$TMPFILE"
    sed -i 's/▎/|/g' "$TMPFILE"
    sed -i 's/░/./g' "$TMPFILE"
    sed -i 's/➖/-/g' "$TMPFILE"
    
    # Emojis - Info/Data
    sed -i 's/📊/[INFO]/g' "$TMPFILE"
    sed -i 's/📋/[LIST]/g' "$TMPFILE"
    sed -i 's/📁/[DIR]/g' "$TMPFILE"
    sed -i 's/📦/[PKG]/g' "$TMPFILE"
    sed -i 's/📜/[LOG]/g' "$TMPFILE"
    sed -i 's/📭/[EMPTY]/g' "$TMPFILE"
    sed -i 's/📝/[NOTE]/g' "$TMPFILE"
    sed -i 's/💡/[TIP]/g' "$TMPFILE"
    
    # Emojis - Actions/Objects
    sed -i 's/🎯/[TARGET]/g' "$TMPFILE"
    sed -i 's/🛡️/[SECURE]/g' "$TMPFILE"
    sed -i 's/🔒/[LOCK]/g' "$TMPFILE"
    sed -i 's/🔓/[UNLOCK]/g' "$TMPFILE"
    sed -i 's/🔍/[SEARCH]/g' "$TMPFILE"
    sed -i 's/🔀/[SWITCH]/g' "$TMPFILE"
    sed -i 's/🔥/[FIRE]/g' "$TMPFILE"
    sed -i 's/💾/[SAVE]/g' "$TMPFILE"
    sed -i 's/🗄️/[DB]/g' "$TMPFILE"
    sed -i 's/🗄/[DB]/g' "$TMPFILE"
    
    # Emojis - Time/Status
    sed -i 's/⏱️/[TIME]/g' "$TMPFILE"
    sed -i 's/⏱/[TIME]/g' "$TMPFILE"
    sed -i 's/⏳/[WAIT]/g' "$TMPFILE"
    sed -i 's/⏪/[REW]/g' "$TMPFILE"
    sed -i 's/⏹️/[STOP]/g' "$TMPFILE"
    sed -i 's/⏹/[STOP]/g' "$TMPFILE"
    sed -i 's/⟳/[SYNC]/g' "$TMPFILE"
    
    # Emojis - Cloud
    sed -i 's/☁️/[CLOUD]/g' "$TMPFILE"
    sed -i 's/☁/[CLOUD]/g' "$TMPFILE"
    sed -i 's/📤/[UPLOAD]/g' "$TMPFILE"
    sed -i 's/📥/[DOWNLOAD]/g' "$TMPFILE"
    sed -i 's/🗑️/[DELETE]/g' "$TMPFILE"
    
    # Emojis - Misc
    sed -i 's/📈/[UP]/g' "$TMPFILE"
    sed -i 's/📉/[DOWN]/g' "$TMPFILE"
    sed -i 's/⌨️/[KEY]/g' "$TMPFILE"
    sed -i 's/⌨/[KEY]/g' "$TMPFILE"
    sed -i 's/⚙️/[CONFIG]/g' "$TMPFILE"
    sed -i 's/⚙/[CONFIG]/g' "$TMPFILE"
    sed -i 's/✏️/[EDIT]/g' "$TMPFILE"
    sed -i 's/✏/[EDIT]/g' "$TMPFILE"
    sed -i 's/⚡/[FAST]/g' "$TMPFILE"
    
    # Spinner characters (braille patterns for loading animations)
    sed -i 's/⠋/|/g' "$TMPFILE"
    sed -i 's/⠙/\//g' "$TMPFILE"
    sed -i 's/⠹/-/g' "$TMPFILE"
    sed -i 's/⠸/\\/g' "$TMPFILE"
    sed -i 's/⠼/|/g' "$TMPFILE"
    sed -i 's/⠴/\//g' "$TMPFILE"
    sed -i 's/⠦/-/g' "$TMPFILE"
    sed -i 's/⠧/\\/g' "$TMPFILE"
    sed -i 's/⠇/|/g' "$TMPFILE"
    sed -i 's/⠏/\//g' "$TMPFILE"
    
    # Move temp file over original
    mv "$TMPFILE" "$file"
done

echo ""
echo "[OK] Replacement complete!"
echo ""

# Verify
REMAINING=$(grep -roP '[\x{80}-\x{FFFF}]' --include="*.go" . 2>/dev/null | wc -l || echo "0")

echo "[INFO] Unicode characters remaining: $REMAINING"
if [ "$REMAINING" -gt 0 ]; then
    echo "[WARN] Some Unicode still exists (might be in comments or safe locations)"
    echo "[INFO] Unique remaining characters:"
    grep -roP '[\x{80}-\x{FFFF}]' --include="*.go" . 2>/dev/null | grep -oP '[\x{80}-\x{FFFF}]' | sort -u | head -20
else
    echo "[OK] All Unicode characters replaced with ASCII!"
fi

echo ""
echo "[INFO] Backup: $BACKUP_DIR"
echo "[INFO] To restore: cp -r $BACKUP_DIR/* ."
echo ""
echo "[INFO] Next steps:"
echo "  1. go build"
echo "  2. go test ./..."
echo "  3. Test TUI: ./dbbackup"
echo "  4. Commit: git add . && git commit -m 'v3.42.11: Replace all Unicode with ASCII'"
echo ""
