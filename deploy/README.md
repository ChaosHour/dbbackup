# Deployment Examples for dbbackup

Enterprise deployment configurations for various platforms and orchestration tools.

## Directory Structure

```
deploy/
├── README.md
├── ansible/              # Ansible roles and playbooks
│   ├── basic.yml         # Simple installation
│   ├── with-exporter.yml # With Prometheus metrics
│   ├── with-notifications.yml # With email/Slack alerts
│   ├── enterprise.yml    # Full enterprise setup
│   └── deploy-production.yml # Production binary deployment
├── kubernetes/           # Kubernetes manifests
│   ├── cronjob.yaml      # Scheduled backup CronJob
│   ├── configmap.yaml    # Configuration
│   ├── pvc.yaml          # Persistent volume claim
│   ├── secret.yaml.example # Secrets template
│   └── servicemonitor.yaml # Prometheus ServiceMonitor
├── prometheus/           # Prometheus configuration
│   ├── alerting-rules.yaml
│   └── scrape-config.yaml
├── systemd/              # Systemd units for GFS pruning
│   ├── dbbackup-prune.service
│   ├── dbbackup-prune.timer
│   └── prune.env
├── terraform/            # Infrastructure as Code
│   └── aws/              # AWS deployment (S3 bucket)
└── scripts/              # Helper scripts
    ├── backup-rotation.sh
    └── health-check.sh
```

## Quick Start by Platform

### Ansible
```bash
cd ansible
cp inventory.example inventory
ansible-playbook -i inventory enterprise.yml
```

### Kubernetes
```bash
kubectl apply -f kubernetes/
```

### Terraform (AWS)
```bash
cd terraform/aws
terraform init
terraform apply
```

## Feature Matrix

| Feature | basic | with-exporter | with-notifications | enterprise |
|---------|:-----:|:-------------:|:------------------:|:----------:|
| Scheduled Backups | ✓ | ✓ | ✓ | ✓ |
| Retention Policy | ✓ | ✓ | ✓ | ✓ |
| GFS Rotation | | | | ✓ |
| Prometheus Metrics | | ✓ | | ✓ |
| Email Notifications | | | ✓ | ✓ |
| Slack/Webhook | | | ✓ | ✓ |
| Encryption | | | | ✓ |
| Cloud Upload | | | | ✓ |
| Catalog Sync | | | | ✓ |
