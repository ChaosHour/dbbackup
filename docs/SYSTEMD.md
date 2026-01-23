# Systemd Integration Guide

This guide covers installing dbbackup as a systemd service for automated scheduled backups.

## Quick Start (Installer)

The easiest way to set up systemd services is using the built-in installer:

```bash
# Install as cluster backup service (daily at midnight)
sudo dbbackup install --backup-type cluster --schedule daily

# Check what would be installed (dry-run)
dbbackup install --dry-run --backup-type cluster

# Check installation status
dbbackup install --status

# Uninstall
sudo dbbackup uninstall cluster --purge
```

## Installer Options

| Flag | Description | Default |
|------|-------------|---------|
| `--instance NAME` | Instance name for named backups | - |
| `--backup-type TYPE` | Backup type: `cluster`, `single`, `sample` | `cluster` |
| `--schedule SPEC` | Timer schedule (see below) | `daily` |
| `--with-metrics` | Install Prometheus metrics exporter | false |
| `--metrics-port PORT` | HTTP port for metrics exporter | 9399 |
| `--dry-run` | Preview changes without applying | false |

### Schedule Format

The `--schedule` option accepts systemd OnCalendar format:

| Value | Description |
|-------|-------------|
| `daily` | Every day at midnight |
| `weekly` | Every Monday at midnight |
| `hourly` | Every hour |
| `*-*-* 02:00:00` | Every day at 2:00 AM |
| `*-*-* 00/6:00:00` | Every 6 hours |
| `Mon *-*-* 03:00` | Every Monday at 3:00 AM |
| `*-*-01 00:00:00` | First day of every month |

Test schedule with: `systemd-analyze calendar "Mon *-*-* 03:00"`

## What Gets Installed

### Directory Structure

```
/etc/dbbackup/
├── dbbackup.conf           # Main configuration
└── env.d/
    └── cluster.conf        # Instance credentials (mode 0600)

/var/lib/dbbackup/
├── catalog/
│   └── backups.db          # SQLite backup catalog
├── backups/                # Default backup storage
└── metrics/                # Prometheus textfile metrics

/var/log/dbbackup/          # Log files

/usr/local/bin/dbbackup     # Binary copy
```

### Systemd Units

**For cluster backups:**
- `/etc/systemd/system/dbbackup-cluster.service` - Backup service
- `/etc/systemd/system/dbbackup-cluster.timer` - Backup scheduler

**For named instances:**
- `/etc/systemd/system/dbbackup@.service` - Template service
- `/etc/systemd/system/dbbackup@.timer` - Template timer

**Metrics exporter (optional):**
- `/etc/systemd/system/dbbackup-exporter.service`

### System User

A dedicated `dbbackup` user and group are created:
- Home: `/var/lib/dbbackup`
- Shell: `/usr/sbin/nologin`
- Purpose: Run backup services with minimal privileges

## Manual Installation

If you prefer to set up systemd services manually without the installer:

### Step 1: Create User and Directories

```bash
# Create system user
sudo useradd --system --home-dir /var/lib/dbbackup --shell /usr/sbin/nologin dbbackup

# Create directories
sudo mkdir -p /etc/dbbackup/env.d
sudo mkdir -p /var/lib/dbbackup/{catalog,backups,metrics}
sudo mkdir -p /var/log/dbbackup

# Set ownership
sudo chown -R dbbackup:dbbackup /var/lib/dbbackup /var/log/dbbackup
sudo chown root:dbbackup /etc/dbbackup
sudo chmod 750 /etc/dbbackup

# Copy binary
sudo cp dbbackup /usr/local/bin/
sudo chmod 755 /usr/local/bin/dbbackup
```

### Step 2: Create Configuration

```bash
# Main configuration in working directory (where service runs from)
# dbbackup reads .dbbackup.conf from WorkingDirectory
sudo tee /var/lib/dbbackup/.dbbackup.conf << 'EOF'
# DBBackup Configuration
db-type=postgres
host=localhost
port=5432
user=postgres
backup-dir=/var/lib/dbbackup/backups
compression=6
retention-days=30
min-backups=7
EOF
sudo chown dbbackup:dbbackup /var/lib/dbbackup/.dbbackup.conf
sudo chmod 600 /var/lib/dbbackup/.dbbackup.conf

# Instance credentials (secure permissions)
sudo tee /etc/dbbackup/env.d/cluster.conf << 'EOF'
PGPASSWORD=your_secure_password
# Or for MySQL:
# MYSQL_PWD=your_secure_password
EOF
sudo chmod 600 /etc/dbbackup/env.d/cluster.conf
sudo chown dbbackup:dbbackup /etc/dbbackup/env.d/cluster.conf
```

### Step 3: Create Service Unit

```bash
sudo tee /etc/systemd/system/dbbackup-cluster.service << 'EOF'
[Unit]
Description=DBBackup Cluster Backup
Documentation=https://github.com/PlusOne/dbbackup
After=network.target postgresql.service mysql.service
Wants=network.target

[Service]
Type=oneshot
User=dbbackup
Group=dbbackup

# Load configuration
EnvironmentFile=-/etc/dbbackup/env.d/cluster.conf

# Working directory (config is loaded from .dbbackup.conf here)
WorkingDirectory=/var/lib/dbbackup

# Execute backup (reads .dbbackup.conf from WorkingDirectory)
ExecStart=/usr/local/bin/dbbackup backup cluster \
    --backup-dir /var/lib/dbbackup/backups \
    --host localhost \
    --port 5432 \
    --user postgres \
    --allow-root

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
MemoryDenyWriteExecute=yes
LockPersonality=yes

# Allow write to specific paths
ReadWritePaths=/var/lib/dbbackup /var/log/dbbackup

# Capability restrictions
CapabilityBoundingSet=CAP_DAC_READ_SEARCH CAP_NET_CONNECT
AmbientCapabilities=

# Resource limits
MemoryMax=4G
CPUQuota=80%

# Prevent OOM killer from terminating backups
OOMScoreAdjust=-100

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=dbbackup

[Install]
WantedBy=multi-user.target
EOF
```

### Step 4: Create Timer Unit

```bash
sudo tee /etc/systemd/system/dbbackup-cluster.timer << 'EOF'
[Unit]
Description=DBBackup Cluster Backup Timer
Documentation=https://github.com/PlusOne/dbbackup

[Timer]
# Run daily at midnight
OnCalendar=daily

# Randomize start time within 15 minutes to avoid thundering herd
RandomizedDelaySec=900

# Run immediately if we missed the last scheduled time
Persistent=true

# Run even if system was sleeping
WakeSystem=false

[Install]
WantedBy=timers.target
EOF
```

### Step 5: Enable and Start

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable timer (auto-start on boot)
sudo systemctl enable dbbackup-cluster.timer

# Start timer
sudo systemctl start dbbackup-cluster.timer

# Verify timer is active
sudo systemctl status dbbackup-cluster.timer

# View next scheduled run
sudo systemctl list-timers dbbackup-cluster.timer
```

### Step 6: Test Backup

```bash
# Run backup manually
sudo systemctl start dbbackup-cluster.service

# Check status
sudo systemctl status dbbackup-cluster.service

# View logs
sudo journalctl -u dbbackup-cluster.service -f
```

## Prometheus Metrics Exporter (Manual)

### Service Unit

```bash
sudo tee /etc/systemd/system/dbbackup-exporter.service << 'EOF'
[Unit]
Description=DBBackup Prometheus Metrics Exporter
Documentation=https://github.com/PlusOne/dbbackup
After=network.target

[Service]
Type=simple
User=dbbackup
Group=dbbackup

# Working directory
WorkingDirectory=/var/lib/dbbackup

# Start HTTP metrics server
ExecStart=/usr/local/bin/dbbackup metrics serve --port 9399

# Restart on failure
Restart=on-failure
RestartSec=10

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes

# Catalog access
ReadWritePaths=/var/lib/dbbackup

# Capability restrictions
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
AmbientCapabilities=

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=dbbackup-exporter

[Install]
WantedBy=multi-user.target
EOF
```

### Enable Exporter

```bash
sudo systemctl daemon-reload
sudo systemctl enable dbbackup-exporter
sudo systemctl start dbbackup-exporter

# Test
curl http://localhost:9399/health
curl http://localhost:9399/metrics
```

### Prometheus Configuration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'dbbackup'
    static_configs:
      - targets: ['localhost:9399']
    scrape_interval: 60s
```

## Security Hardening

The systemd units include comprehensive security hardening:

| Setting | Purpose |
|---------|---------|
| `NoNewPrivileges=yes` | Prevent privilege escalation |
| `ProtectSystem=strict` | Read-only filesystem except allowed paths |
| `ProtectHome=yes` | Block access to /home, /root, /run/user |
| `PrivateTmp=yes` | Isolated /tmp namespace |
| `PrivateDevices=yes` | No access to physical devices |
| `RestrictAddressFamilies` | Only Unix and IP sockets |
| `MemoryDenyWriteExecute=yes` | Prevent code injection |
| `CapabilityBoundingSet` | Minimal Linux capabilities |
| `OOMScoreAdjust=-100` | Protect backup from OOM killer |

### Database Access

For PostgreSQL with peer authentication:
```bash
# Add dbbackup user to postgres group
sudo usermod -aG postgres dbbackup

# Or create a .pgpass file
sudo -u dbbackup tee /var/lib/dbbackup/.pgpass << EOF
localhost:5432:*:postgres:password
EOF
sudo chmod 600 /var/lib/dbbackup/.pgpass
```

For PostgreSQL with password authentication:
```bash
# Store password in environment file
echo "PGPASSWORD=your_password" | sudo tee /etc/dbbackup/env.d/cluster.conf
sudo chmod 600 /etc/dbbackup/env.d/cluster.conf
```

## Multiple Instances

Run different backup configurations as separate instances:

```bash
# Install multiple instances
sudo dbbackup install --instance production --schedule "*-*-* 02:00:00"
sudo dbbackup install --instance staging --schedule "*-*-* 04:00:00"
sudo dbbackup install --instance analytics --schedule "weekly"

# Manage individually
sudo systemctl status dbbackup@production.timer
sudo systemctl start dbbackup@staging.service
```

Each instance has its own:
- Configuration: `/etc/dbbackup/env.d/<instance>.conf`
- Timer schedule
- Journal logs: `journalctl -u dbbackup@<instance>.service`

## Troubleshooting

### View Logs

```bash
# Real-time logs
sudo journalctl -u dbbackup-cluster.service -f

# Last backup run
sudo journalctl -u dbbackup-cluster.service -n 100

# All dbbackup logs
sudo journalctl -t dbbackup

# Exporter logs
sudo journalctl -u dbbackup-exporter -f
```

### Timer Not Running

```bash
# Check timer status
sudo systemctl status dbbackup-cluster.timer

# List all timers
sudo systemctl list-timers --all | grep dbbackup

# Check if timer is enabled
sudo systemctl is-enabled dbbackup-cluster.timer
```

### Service Fails to Start

```bash
# Check service status
sudo systemctl status dbbackup-cluster.service

# View detailed error
sudo journalctl -u dbbackup-cluster.service -n 50 --no-pager

# Test manually as dbbackup user (run from working directory with .dbbackup.conf)
cd /var/lib/dbbackup && sudo -u dbbackup /usr/local/bin/dbbackup backup cluster

# Check permissions
ls -la /var/lib/dbbackup/
ls -la /var/lib/dbbackup/.dbbackup.conf
```

### Permission Denied

```bash
# Fix ownership
sudo chown -R dbbackup:dbbackup /var/lib/dbbackup

# Check SELinux (if enabled)
sudo ausearch -m avc -ts recent

# Check AppArmor (if enabled)
sudo aa-status
```

### Exporter Not Accessible

```bash
# Check if running
sudo systemctl status dbbackup-exporter

# Check port binding
sudo ss -tlnp | grep 9399

# Test locally
curl -v http://localhost:9399/health

# Check firewall
sudo ufw status
sudo iptables -L -n | grep 9399
```

## Prometheus Alerting Rules

Add these alert rules to your Prometheus configuration for backup monitoring:

```yaml
# /etc/prometheus/rules/dbbackup.yml
groups:
  - name: dbbackup
    rules:
      # Alert if no successful backup in 24 hours
      - alert: DBBackupMissing
        expr: time() - dbbackup_last_success_timestamp > 86400
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "No backup in 24 hours on {{ $labels.instance }}"
          description: "Database {{ $labels.database }} has not had a successful backup in over 24 hours."

      # Alert if backup verification failed
      - alert: DBBackupVerificationFailed
        expr: dbbackup_backup_verified == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Backup verification failed on {{ $labels.instance }}"
          description: "Last backup for {{ $labels.database }} failed verification check."

      # Alert if RPO exceeded (48 hours)
      - alert: DBBackupRPOExceeded
        expr: dbbackup_rpo_seconds > 172800
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "RPO exceeded on {{ $labels.instance }}"
          description: "Recovery Point Objective exceeded 48 hours for {{ $labels.database }}."

      # Alert if exporter is down
      - alert: DBBackupExporterDown
        expr: up{job="dbbackup"} == 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DBBackup exporter down on {{ $labels.instance }}"
          description: "Cannot scrape metrics from dbbackup-exporter."

      # Alert if backup size dropped significantly (possible truncation)
      - alert: DBBackupSizeAnomaly
        expr: dbbackup_last_backup_size_bytes < (dbbackup_last_backup_size_bytes offset 1d) * 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Backup size anomaly on {{ $labels.instance }}"
          description: "Backup size for {{ $labels.database }} dropped by more than 50%."
```

### Loading Alert Rules

```bash
# Test rules syntax
promtool check rules /etc/prometheus/rules/dbbackup.yml

# Reload Prometheus
sudo systemctl reload prometheus
# or via API:
curl -X POST http://localhost:9090/-/reload
```

## Catalog Sync for Existing Backups

If you have existing backups created before installing v3.41+, sync them to the catalog:

```bash
# Sync existing backups to catalog
dbbackup catalog sync /path/to/backup/directory --allow-root

# Verify catalog contents
dbbackup catalog list --allow-root

# Show statistics
dbbackup catalog stats --allow-root
```

## Uninstallation

### Using Installer

```bash
# Remove cluster backup (keeps config)
sudo dbbackup uninstall cluster

# Remove and purge configuration
sudo dbbackup uninstall cluster --purge

# Remove named instance
sudo dbbackup uninstall production --purge
```

### Manual Removal

```bash
# Stop and disable services
sudo systemctl stop dbbackup-cluster.timer dbbackup-cluster.service dbbackup-exporter
sudo systemctl disable dbbackup-cluster.timer dbbackup-exporter

# Remove unit files
sudo rm /etc/systemd/system/dbbackup-cluster.service
sudo rm /etc/systemd/system/dbbackup-cluster.timer
sudo rm /etc/systemd/system/dbbackup-exporter.service
sudo rm /etc/systemd/system/dbbackup@.service
sudo rm /etc/systemd/system/dbbackup@.timer

# Reload systemd
sudo systemctl daemon-reload

# Optional: Remove user and directories
sudo userdel dbbackup
sudo rm -rf /var/lib/dbbackup
sudo rm -rf /etc/dbbackup
sudo rm -rf /var/log/dbbackup
sudo rm /usr/local/bin/dbbackup
```

## See Also

- [README.md](README.md) - Main documentation
- [DOCKER.md](DOCKER.md) - Docker deployment
- [CLOUD.md](CLOUD.md) - Cloud storage configuration
- [PITR.md](PITR.md) - Point-in-Time Recovery
