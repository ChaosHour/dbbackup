# Restore Profiles

## Overview

The `--profile` flag allows you to optimize restore operations based on your server's resources and current workload. This is particularly useful when dealing with "out of shared memory" errors or resource-constrained environments.

## Available Profiles

### Conservative Profile (`--profile=conservative`)
**Best for:** Resource-constrained servers, production systems with other running services, or when dealing with "out of shared memory" errors.

**Settings:**
- Single-threaded restore (`--parallel=1`)
- Single-threaded decompression (`--jobs=1`)
- Memory-conservative mode enabled
- Minimal memory footprint

**When to use:**
- Server RAM usage > 70%
- Other critical services running (web servers, monitoring agents)
- "out of shared memory" errors during restore
- Small VMs or shared hosting environments
- Disk I/O is the bottleneck

**Example:**
```bash
dbbackup restore cluster backup.tar.gz --profile=conservative --confirm
```

### Balanced Profile (`--profile=balanced`) - DEFAULT
**Best for:** Most scenarios, general-purpose servers with adequate resources.

**Settings:**
- Auto-detect parallelism based on CPU/RAM
- Moderate resource usage
- Good balance between speed and stability

**When to use:**
- Default choice for most restores
- Dedicated database server with moderate load
- Unknown or variable server conditions

**Example:**
```bash
dbbackup restore cluster backup.tar.gz --confirm
# or explicitly:
dbbackup restore cluster backup.tar.gz --profile=balanced --confirm
```

### Aggressive Profile (`--profile=aggressive`)
**Best for:** Dedicated database servers with ample resources, maintenance windows, performance-critical restores.

**Settings:**
- Maximum parallelism (auto-detect based on CPU cores)
- Maximum resource utilization
- Fastest restore speed

**When to use:**
- Dedicated database server (no other services)
- Server RAM usage < 50%
- Time-critical restores (RTO minimization)
- Maintenance windows with service downtime
- Testing/development environments

**Example:**
```bash
dbbackup restore cluster backup.tar.gz --profile=aggressive --confirm
```

### Potato Profile (`--profile=potato`)
**Easter egg:** Same as conservative, for servers running on a potato.

### Turbo Profile (`--profile=turbo`)
**NEW! Best for:** Maximum restore speed - matches native pg_restore -j8 performance.

**Settings:**
- Parallel databases: 2 (balanced I/O)
- pg_restore jobs: 8 (like `pg_restore -j8`)
- Buffered I/O: 32KB write buffers for faster extraction
- Optimized for large databases

**When to use:**
- Dedicated database server
- Need fastest possible restore (DR scenarios)
- Server has 16GB+ RAM, 4+ cores
- Large databases (100GB+)
- You want dbbackup to match pg_restore speed

**Example:**
```bash
dbbackup restore cluster backup.tar.gz --profile=turbo --confirm
```

**TUI Usage:**
1. Go to Settings → Resource Profile
2. Press Enter to cycle until you see "turbo"
3. Save settings and run restore

## Profile Comparison

| Setting | Conservative | Balanced | Performance | Turbo |
|---------|-------------|----------|-------------|----------|
| Parallel DBs | 1 | 2 | 4 | 2 |
| pg_restore Jobs | 1 | 2 | 4 | 8 |
| Buffered I/O | No | No | No | Yes (32KB) |
| Memory Usage | Minimal | Moderate | High | Moderate |
| Speed | Slowest | Medium | Fast | **Fastest** |
| Stability | Most stable | Stable | Good | Good |
| Best For | Small VMs | General use | Powerful servers | DR/Large DBs |

## Overriding Profile Settings

You can override specific profile settings:

```bash
# Use conservative profile but allow 2 parallel jobs for decompression
dbbackup restore cluster backup.tar.gz \\
  --profile=conservative \\
  --jobs=2 \\
  --confirm

# Use aggressive profile but limit to 2 parallel databases
dbbackup restore cluster backup.tar.gz \\
  --profile=aggressive \\
  --parallel-dbs=2 \\
  --confirm
```

## Real-World Scenarios

### Scenario 1: "Out of Shared Memory" Error
**Problem:** PostgreSQL restore fails with `ERROR: out of shared memory`

**Solution:**
```bash
# Step 1: Use conservative profile
dbbackup restore cluster backup.tar.gz --profile=conservative --confirm

# Step 2: If still failing, temporarily stop monitoring agents
sudo systemctl stop nessus-agent elastic-agent
dbbackup restore cluster backup.tar.gz --profile=conservative --confirm
sudo systemctl start nessus-agent elastic-agent

# Step 3: Ask infrastructure team to increase work_mem (see email_infra_team.txt)
```

### Scenario 2: Fast Disaster Recovery
**Goal:** Restore as quickly as possible during maintenance window

**Solution:**
```bash
# Stop all non-essential services first
sudo systemctl stop nginx php-fpm
dbbackup restore cluster backup.tar.gz --profile=aggressive --confirm
sudo systemctl start nginx php-fpm
```

### Scenario 3: Shared Server with Multiple Services
**Environment:** Web server + database + monitoring all on same VM

**Solution:**
```bash
# Always use conservative to avoid impacting other services
dbbackup restore cluster backup.tar.gz --profile=conservative --confirm
```

### Scenario 4: Unknown Server Conditions
**Situation:** Restoring to a new server, unsure of resources

**Solution:**
```bash
# Step 1: Run diagnostics first
./diagnose_postgres_memory.sh > diagnosis.log

# Step 2: Choose profile based on memory usage:
# - If memory > 80%: use conservative
# - If memory 50-80%: use balanced (default)
# - If memory < 50%: use aggressive

# Step 3: Start with balanced and adjust if needed
dbbackup restore cluster backup.tar.gz --confirm
```

## Troubleshooting

### Profile Selection Guide

**Use Conservative when:**
- Memory usage > 70%
- Other services running
- Getting "out of shared memory" errors
- Restore keeps failing
- Small VM (< 4 GB RAM)
- High swap usage

**Use Balanced when:**
- Normal operation
- Moderate server load
- Unsure what to use
- Medium VM (4-16 GB RAM)

**Use Aggressive when:**
- Dedicated database server
- Memory usage < 50%
- No other critical services
- Need fastest possible restore
- Large VM (> 16 GB RAM)
- Maintenance window

## Environment Variables

You can set a default profile:

```bash
export RESOURCE_PROFILE=conservative
dbbackup restore cluster backup.tar.gz --confirm
```

## See Also

- [diagnose_postgres_memory.sh](diagnose_postgres_memory.sh) - Analyze system resources before restore
- [fix_postgres_locks.sh](fix_postgres_locks.sh) - Fix PostgreSQL lock exhaustion
- [email_infra_team.txt](email_infra_team.txt) - Template email for infrastructure team
