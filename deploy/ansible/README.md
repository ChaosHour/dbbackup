# Ansible Deployment for dbbackup

Ansible roles and playbooks for deploying dbbackup in enterprise environments.

## Playbooks

| Playbook | Description |
|----------|-------------|
| `basic.yml` | Simple installation without monitoring |
| `with-exporter.yml` | Installation with Prometheus metrics exporter |
| `with-notifications.yml` | Installation with SMTP/webhook notifications |
| `enterprise.yml` | Full enterprise setup (exporter + notifications + GFS retention) |

## Quick Start

```bash
# Edit inventory
cp inventory.example inventory
vim inventory

# Edit variables
vim group_vars/all.yml

# Deploy basic setup
ansible-playbook -i inventory basic.yml

# Deploy enterprise setup
ansible-playbook -i inventory enterprise.yml
```

## Variables

See `group_vars/all.yml` for all configurable options.

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `dbbackup_version` | Version to install | `3.42.74` |
| `dbbackup_db_type` | Database type | `postgres` or `mysql` |
| `dbbackup_backup_dir` | Backup storage path | `/var/backups/databases` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `dbbackup_schedule` | Backup schedule | `daily` |
| `dbbackup_compression` | Compression level | `6` |
| `dbbackup_retention_days` | Retention period | `30` |
| `dbbackup_min_backups` | Minimum backups to keep | `5` |
| `dbbackup_exporter_port` | Prometheus exporter port | `9399` |

## Directory Structure

```
ansible/
├── README.md
├── inventory.example
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
