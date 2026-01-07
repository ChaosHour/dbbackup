# v3.41.0 Release Notes

## What's New in v3.41.0

### Features
- **Systemd Integration** - One-command install with `dbbackup install`
- **Prometheus Metrics** - HTTP exporter on port 9399 with `/metrics` and `/health` endpoints
- **Backup Catalog** - SQLite-based tracking of all backup operations
- **Automated CI/CD** - Gitea Actions pipeline with automated releases

### Installation

#### Quick Install (Recommended)
```bash
# Download for your platform
curl -LO https://git.uuxo.net/UUXO/dbbackup/releases/download/v3.41.0/dbbackup-linux-amd64

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
- Fixed exporter status detection in `install --status`
- Improved error handling in restore operations
- Better JSON escaping in CI release creation

---

**Full Changelog**: https://git.uuxo.net/UUXO/dbbackup/compare/v3.40.0...v3.41.0
