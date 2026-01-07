# v3.42.0 Release Notes

## What's New in v3.42.0

### Deduplication - Resistance is Futile

Content-defined chunking deduplication for space-efficient backups. Like restic/borgbackup but with **native database dump support**.

```bash
# First backup: 5MB stored
dbbackup dedup backup mydb.dump

# Second backup (modified): only 1.6KB new data stored!
# 100% deduplication ratio
dbbackup dedup backup mydb_modified.dump
```

#### Features
- **Gear Hash CDC** - Content-defined chunking with 92%+ overlap on shifted data
- **SHA-256 Content-Addressed** - Chunks stored by hash, automatic deduplication
- **AES-256-GCM Encryption** - Optional per-chunk encryption
- **Gzip Compression** - Optional compression (enabled by default)
- **SQLite Index** - Fast chunk lookups and statistics

#### Commands
```bash
dbbackup dedup backup <file>              # Create deduplicated backup
dbbackup dedup backup <file> --encrypt    # With AES-256-GCM encryption
dbbackup dedup restore <id> <output>      # Restore from manifest
dbbackup dedup list                       # List all backups
dbbackup dedup stats                      # Show deduplication statistics
dbbackup dedup delete <id>                # Delete a backup
dbbackup dedup gc                         # Garbage collect unreferenced chunks
```

#### Storage Structure
```
<backup-dir>/dedup/
  chunks/           # Content-addressed chunk files
    ab/cdef1234...  # Sharded by first 2 chars of hash
  manifests/        # JSON manifest per backup
  chunks.db         # SQLite index
```

### Also Included (from v3.41.x)
- **Systemd Integration** - One-command install with `dbbackup install`
- **Prometheus Metrics** - HTTP exporter on port 9399
- **Backup Catalog** - SQLite-based tracking of all backup operations
- **Prometheus Alerting Rules** - Added to SYSTEMD.md documentation

### Installation

#### Quick Install (Recommended)
```bash
# Download for your platform
curl -LO https://git.uuxo.net/UUXO/dbbackup/releases/download/v3.42.0/dbbackup-linux-amd64

# Install with systemd service
chmod +x dbbackup-linux-amd64
sudo ./dbbackup-linux-amd64 install --config /path/to/config.yaml
```

#### Available Binaries
| Platform | Architecture | Binary |
|----------|--------------|--------|
| Linux | amd64 | `dbbackup-linux-amd64` |
| Linux | arm64 | `dbbackup-linux-arm64` |
| macOS | Intel | `dbbackup-darwin-amd64` |
| macOS | Apple Silicon | `dbbackup-darwin-arm64` |
| FreeBSD | amd64 | `dbbackup-freebsd-amd64` |

### Systemd Commands
```bash
dbbackup install --config config.yaml   # Install service + timer
dbbackup install --status               # Check service status
dbbackup install --uninstall            # Remove services
```

### Prometheus Metrics
Available at `http://localhost:9399/metrics`:

| Metric | Description |
|--------|-------------|
| `dbbackup_last_backup_timestamp` | Unix timestamp of last backup |
| `dbbackup_last_backup_success` | 1 if successful, 0 if failed |
| `dbbackup_last_backup_duration_seconds` | Duration of last backup |
| `dbbackup_last_backup_size_bytes` | Size of last backup |
| `dbbackup_backup_total` | Total number of backups |
| `dbbackup_backup_errors_total` | Total number of failed backups |

### Security Features
- Hardened systemd service with `ProtectSystem=strict`
- `NoNewPrivileges=true` prevents privilege escalation
- Dedicated `dbbackup` system user (optional)
- Credential files with restricted permissions

### Documentation
- [SYSTEMD.md](SYSTEMD.md) - Complete systemd installation guide
- [README.md](README.md) - Full documentation
- [CHANGELOG.md](CHANGELOG.md) - Version history

### Bug Fixes
- Fixed SQLite time parsing in dedup stats
- Fixed function name collision in cmd package

---

**Full Changelog**: https://git.uuxo.net/UUXO/dbbackup/compare/v3.41.1...v3.42.0
