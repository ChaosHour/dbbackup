# dbbackup Kubernetes Deployment

Kubernetes manifests for running dbbackup as scheduled CronJobs.

## Quick Start

```bash
# Create namespace
kubectl create namespace dbbackup

# Create secrets
kubectl create secret generic dbbackup-db-credentials \
  --namespace dbbackup \
  --from-literal=password=your-db-password

# Apply manifests
kubectl apply -f . --namespace dbbackup

# Check CronJob
kubectl get cronjobs -n dbbackup
```

## Components

- `configmap.yaml` - Configuration settings
- `secret.yaml` - Credentials template (use kubectl create secret instead)
- `cronjob.yaml` - Scheduled backup job
- `pvc.yaml` - Persistent volume for backup storage
- `servicemonitor.yaml` - Prometheus ServiceMonitor (optional)

## Customization

Edit `configmap.yaml` to configure:
- Database connection
- Backup schedule
- Retention policy
- Cloud storage

## Helm Chart

For more complex deployments, use the Helm chart:

```bash
helm install dbbackup ./helm/dbbackup \
  --set database.host=postgres.default.svc \
  --set database.password=secret \
  --set schedule="0 2 * * *"
```
