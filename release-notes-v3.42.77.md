# dbbackup v3.42.77

## 🎯 New Feature: Single Database Extraction from Cluster Backups

Extract and restore individual databases from cluster backups without full cluster restoration!

### 🆕 New Flags

- **`--list-databases`**: List all databases in cluster backup with sizes
- **`--database <name>`**: Extract/restore a single database from cluster
- **`--databases "db1,db2,db3"`**: Extract multiple databases (comma-separated)
- **`--output-dir <path>`**: Extract to directory without restoring
- **`--target <name>`**: Rename database during restore

### 📖 Examples

```bash
# List databases in cluster backup
dbbackup restore cluster backup.tar.gz --list-databases

# Extract single database (no restore)
dbbackup restore cluster backup.tar.gz --database myapp --output-dir /tmp/extract

# Restore single database from cluster
dbbackup restore cluster backup.tar.gz --database myapp --confirm

# Restore with different name (testing)
dbbackup restore cluster backup.tar.gz --database myapp --target myapp_test --confirm

# Extract multiple databases
dbbackup restore cluster backup.tar.gz --databases "app1,app2,app3" --output-dir /tmp/extract
```

### 💡 Use Cases

✅ **Selective disaster recovery** - restore only affected databases  
✅ **Database migration** - copy databases between clusters  
✅ **Testing workflows** - restore with different names  
✅ **Faster restores** - extract only what you need  
✅ **Less disk space** - no need to extract entire cluster  

### ⚙️ Technical Details

- Stream-based extraction with progress feedback
- Fast cluster archive scanning (no full extraction needed)
- Works with all cluster backup formats (.tar.gz)
- Compatible with existing cluster restore workflow
- Automatic format detection for extracted dumps

### 🖥️ TUI Support (Interactive Mode)

**New in this release**: Press **`s`** key when viewing a cluster backup to select individual databases!

- Navigate cluster backups in TUI and press `s` for database selection
- Interactive database picker with size information
- Visual selection confirmation before restore
- Seamless integration with existing TUI workflows

**TUI Workflow:**
1. Launch TUI: `dbbackup` (no arguments)
2. Navigate to "Restore" → "Single Database"
3. Select cluster backup archive
4. Press `s` to show database list
5. Select database and confirm restore

## 📦 Installation

Download the binary for your platform below and make it executable:

```bash
chmod +x dbbackup_*
./dbbackup_* --version
```

## 🔍 Checksums

SHA256 checksums in `checksums.txt`.
