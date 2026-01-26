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
│   └── enterprise.yml    # Full enterprise setup
├── kubernetes/           # Kubernetes manifests
│   ├── cronjob.yaml      # Scheduled backup CronJob
│   ├── configmap.yaml    # Configuration
│   └── helm/             # Helm chart
├── terraform/            # Infrastructure as Code
│   ├── aws/              # AWS deployment
│   └── gcp/              # GCP deployment
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
# or with Helm
helm install dbbackup kubernetes/helm/dbbackup
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
