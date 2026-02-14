# Ansible Deployment for dbbackup

Ansible roles and playbooks for deploying dbbackup in enterprise environments.

## Playbooks

| Playbook | Description |
|----------|-------------|
| `basic.yml` | Simple installation without monitoring |
| `with-exporter.yml` | Installation with Prometheus metrics exporter |
| `with-notifications.yml` | Installation with SMTP/webhook notifications |
| `enterprise.yml` | Full enterprise setup (exporter + notifications + GFS retention) |
| `deploy-production.yml` | Production deployment: copy binary + verify backup jobs |

## Quick Start

```bash
# Edit inventory
cp inventory.example inventory.yml
vim inventory.yml

# Edit variables
vim group_vars/all.yml

# Deploy basic setup
ansible-playbook -i inventory.yml basic.yml

# Deploy enterprise setup
ansible-playbook -i inventory.yml enterprise.yml

# Production binary deployment (copy pre-built binary + verify)
ansible-playbook -i inventory.yml deploy-production.yml
```

## Variables

See `group_vars/all.yml` for all configurable options.

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `dbbackup_version` | Version to install | `6.36.0` |
| `dbbackup_db_type` | Database type | `postgres`, `mariadb`, or `mysql` |
| `dbbackup_backup_dir` | Backup storage path | `/var/backups/databases` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `dbbackup_schedule` | Backup schedule | `daily` |
| `dbbackup_compression` | Compression level | `6` |
| `dbbackup_retention_days` | Retention period | `30` |
| `dbbackup_min_backups` | Minimum backups to keep | `5` |
| `dbbackup_exporter_port` | Prometheus exporter port | `9399` |
| `dbbackup_allow_root` | Allow running as root | `true` |
| `dbbackup_socket` | Unix socket path (MariaDB) | `/var/run/mysqld/mysqld.sock` |
| `dbbackup_pitr_enabled` | Enable Point-in-Time Recovery | `false` |
| `dbbackup_dedup_enabled` | Enable deduplication | `false` |

## Deployment Strategies

### Copy from Controller (recommended)
Pre-download the binary to the Ansible controller, then deploy via `copy`. Set `dbbackup_binary_src` in inventory:
```yaml
dbbackup_binary_src: "/tmp/dbbackup_linux_amd64"
```

### Download from GitHub
If `dbbackup_binary_src` is not set, the role downloads directly from GitHub Releases:
```
https://github.com/PlusOne/dbbackup/releases/download/v6.36.0/dbbackup_linux_amd64
```

## Directory Structure

```
ansible/
├── README.md
├── deploy-production.yml # Production binary deployment
├── inventory.example
├── inventory.yml         # Production inventory
├── group_vars/
│   └── all.yml
├── roles/
│   └── dbbackup/
│       ├── tasks/
│       │   └── main.yml
│       ├── templates/
│       │   ├── dbbackup.conf.j2
│       │   ├── env.j2
│       │   └── systemd-override.conf.j2
│       └── handlers/
│           └── main.yml
├── basic.yml
├── with-exporter.yml
├── with-notifications.yml
└── enterprise.yml
```
