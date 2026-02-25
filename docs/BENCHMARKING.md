# Benchmarking

Built-in benchmark framework for measuring backup, restore, and verify performance with statistical rigor. Runs multiple iterations and reports min/avg/median/p95/stddev, throughput in MB/s, peak RSS, and compression ratios. Results are saved to the catalog DB and to JSON/Markdown files.

## Quick Start

```bash
# Benchmark PostgreSQL with 3 iterations
dbbackup benchmark run --db-type postgres --database mydb --iterations 3

# Benchmark MySQL with zstd compression
dbbackup benchmark run --db-type mysql --database mydb --compression zstd

# Cross-engine comparison (auto-detects qa_*/bench_* databases)
dbbackup benchmark matrix --iterations 2

# View past results
dbbackup benchmark history --last 10

# Drill into a specific run (JSON output)
dbbackup benchmark show <run-id> --json
```

## CLI Commands

```bash
dbbackup benchmark run mydb --engine postgres --iterations 3
dbbackup benchmark run mydb --engine mysql --iterations 3
dbbackup benchmark run mydb --engine mariadb --iterations 3
dbbackup benchmark matrix --iterations 2    # Cross-engine matrix
```

Environment variables: `BENCH_HOST`, `BENCH_PORT`, `BENCH_USER`, `BENCH_WORKERS`, `BENCH_COMP`, `BENCH_SOCKET`.

## Makefile Targets

```bash
make bench                           # Full cross-engine matrix
make bench-pg BENCH_DB=mydb          # PostgreSQL only
make bench-mysql BENCH_DB=mydb       # MySQL only
make bench-maria BENCH_DB=mydb       # MariaDB only
make bench-history                   # View past results
```

## Sample Output

```
╔════════════════════════════════════════════════════════════╗
║                   BENCHMARK RESULTS                        ║
╠════════════════════════════════════════════════════════════╣
║  Engine:     postgres                                      ║
║  Database:   mydb                                          ║
║  DB Size:    5319.0 MB                                     ║
║  Iterations: 3                                             ║
╠════════════════════════════════════════════════════════════╣
║  Phase     Min       Avg       Median    P95      MB/s     ║
║  backup     40.12s    42.47s    42.19s    44.80s  125.3    ║
║  restore   148.50s   152.66s   151.90s   156.20s   34.8   ║
║  verify      4.20s     4.55s     4.50s     4.90s    —      ║
╚════════════════════════════════════════════════════════════╝
```

Results are persisted to:
- **Catalog DB** (`~/.config/dbbackup/catalog.db`) — queryable via `benchmark history` / `benchmark show`
- **JSON** (`reports/<run_id>.json`) — machine-readable, includes per-iteration detail and peak RSS
- **Markdown** (`reports/<run_id>.md`) — human-readable summary table

## Benchmark Flags

| Flag | Description | Default |
|------|-------------|--------|
| `--iterations` | Number of benchmark iterations | 3 |
| `--workers` | Parallel workers | auto (CPU count) |
| `--compression` | Compression algorithm (gzip/zstd/none) | gzip |
| `--compression-level` | Compression level (0-9) | 6 |
| `--profile` | Resource profile | balanced |
| `--verify` | Run verify phase after backup | true |
| `--clean` | Remove backups between iterations | true |
| `--dump-format` | PostgreSQL dump format (custom/plain/directory) | — |
| `--native` | Use native Go engine | true |
| `--json` | Print JSON to stdout | false |
| `--catalog-db` | Custom catalog DB path | auto |

## 100 GB Native Backup Benchmark (Feb 2026)

Real-world benchmark on a Hetzner dedicated server, 3 runs per engine, native backup to local disk:

![100 GB Benchmark Results](benchmark-100gb.svg)

| Engine | DB Size | Backup Size | Avg Duration | Avg Throughput | Compression |
|--------|---------|-------------|-------------|----------------|-------------|
| PostgreSQL | 103.66 GB | 517.79 MB | 13m 31s | 130.9 MB/s | 205x |
| MariaDB | 97.88 GB | 209.67 MB | 21m 33s | 77.5 MB/s | 478x |
| MySQL | 104.58 GB | 151.08 MB | 11m 09s | 160.2 MB/s | 709x |

- **Best throughput**: MySQL at ~160 MB/s
- **Best compression**: MySQL at 709x (104.58 GB -> 151 MB)
- **Consistent**: < 3% variance across runs per engine
- **Total runtime**: 4h 17m (includes DB creation + 9 backup runs)

## QA Benchmark -- Backup + Restore + Verify (Feb 2026)

Full-cycle benchmark (backup -> restore -> verify), 3 iterations, gzip level 6, 16 workers:

| Engine | DB Size | Backup Avg | Restore Avg | Verify Avg | Total Avg |
|--------|---------|-----------|-------------|------------|-----------|
| PostgreSQL | 25.4 MB | 0.25s | 0.19s | 0.05s | 0.49s |
| MariaDB | 158.6 MB | 5.47s | 11.04s | 0.04s | 16.55s |
| MySQL | 158.6 MB | 5.76s | 10.18s | 0.04s | 15.98s |

- **All iterations passed** (0 failures across all engines)
- **System**: linux/amd64, 16 CPUs, Go 1.24.9

## Automatic Performance Optimizations (v6.1+)

The following optimizations are applied automatically and degrade gracefully on older systems:

| Optimization | Improvement | How It Works |
|-------------|-------------|--------------|
| pgx Batch Pipeline | 15-30% faster DDL | Sends all batch statements in one network round-trip |
| WAL Compression | 10-20% less I/O | Compresses WAL during write-heavy restore phases |
| Unix Socket Auto-detect | 10-30% lower latency | Prefers local socket over TCP for localhost connections |
| BLOB-Aware Buffers | 20-40% faster BLOBs | Dynamically scales buffer size based on detected BLOB characteristics |
| BLOB Type Detection | 20-50% less I/O | Skips compressing pre-compressed BLOBs (JPEG, PNG, GZIP, ZSTD, MP4) |
| Content-Addressed Dedup | Up to 60% less storage | Bloom filter + SHA-256 eliminates duplicate BLOBs across tables |
| Split Backup Mode | 2-5x faster BLOB restore | Schema/data/BLOBs in separate files for parallel phase restore |
| Prepared Statement Cache | 5-10% faster init | Reuses server-side prepared statements for metadata queries |
| MySQL `--quick` + `--extended-insert` | 2-5x faster dump | Row-by-row transfer + multi-row INSERTs reduce overhead |
| MySQL `--init-command` fast restore | 2-5x faster import | Disables FK/unique checks and binary logging during bulk load |
| MySQL 5000-row batch INSERTs | 20-40% faster native | Larger INSERT batches reduce per-statement overhead |
| Vendor-Aware CPU Tuning | 10-25% faster backup | AMD gets aggressive parallelism, Intel uses 75% cores with larger batches, ARM optimized for NEON/SVE |
| ISA-Based Compression Selection | 5-15% faster compress | Auto-selects zstd when AVX2/AVX-512/SVE detected, gzip otherwise |
| Cache-Aware Buffer Sizing | 5-10% less cache thrash | Buffer and batch sizes derived from L2/L3 cache topology per core |
| Hybrid P/E-Core Detection | 10-20% better throughput | Intel 12th+ gen: schedules work on P-cores, avoids E-core bottleneck |
| NUMA-Aware Worker Distribution | 5-15% on multi-socket | Workers distributed 70/30 toward preferred NUMA node to reduce cross-node traffic |
| Frequency Governor Warning | Avoids 2-3x slowdown | Warns when CPU governor is 'powersave' and recommends 'performance' |

**Expected combined impact by workload:**

| Workload | Improvement |
|----------|-------------|
| Small tables (< 100MB, many DDL) | **+25-35%** |
| Large tables (> 1GB, data-heavy) | **+30-60%** |
| BLOB-heavy (images, PDFs) | **+40-100%** |
| Mixed production workload | **+30-45%** |

All optimizations are hardware-independent and adapt to VPS (2 vCPU) through bare metal (64+ cores).

## CPU Architecture Optimization (v6.46.0+)

dbbackup performs deep hardware introspection to squeeze maximum performance from your specific CPU:

```bash
# View your system's CPU optimization report
dbbackup cpu

# Example output on AMD EPYC:
#   Vendor: AuthenticAMD (AMD Server/Desktop)
#   ISA Features: SSE4.2 AVX AVX2 AES-NI PCLMULQDQ
#   GOAMD64 Level: v3 (AVX2+BMI2 -- use _v3 binary for best performance)
#   Recommended Compression: zstd (hardware-accelerated)
#   Cache: L1d=32KB L2=512KB L3=16MB -> Buffer=256KB, Batch=7500
#   Jobs: 16 (aggressive AMD tuning), DumpJobs: 12
#   Governor: performance
#   NUMA: 1 node, 16 CPUs, 32GB
```

**Key capabilities:**

| Feature | Intel | AMD | ARM |
|---------|-------|-----|-----|
| ISA detection (AVX2/512/NEON/SVE) | yes | yes | yes |
| Hybrid P/E-core topology | yes (12th gen+) | -- | -- |
| Vendor-tuned parallelism | 75% cores | All cores | All cores |
| AVX-512 throttle warning | yes (pre-Sapphire Rapids) | -- | -- |
| Cache-aware buffer sizing | yes | yes | yes |
| NUMA worker distribution | yes | yes | -- |
| Frequency governor check | yes | yes | yes |
| Memory bandwidth estimate | yes | yes | yes |

**CLI flags:**

| Flag | Description | Default |
|------|-------------|---------|
| `--cpu-auto-tune` | Enable vendor-aware auto-tuning | `true` |
| `--cpu-boost` | Set governor to 'performance' during backup | `false` |

### GOAMD64=v3 Optimized Binary

For 2015+ Intel (Broadwell) and AMD (Excavator/Zen) CPUs, a dedicated binary compiled with `GOAMD64=v3` enables AVX2 and BMI2 instructions for 5-15% faster compression and hashing:

```bash
# Download the v3 binary (Linux only)
wget https://github.com/PlusOne/dbbackup/releases/latest/download/dbbackup_linux_amd64_v3
chmod +x dbbackup_linux_amd64_v3

# Check if your CPU supports it
dbbackup cpu  # Look for "GOAMD64 Level: v3" or higher
```

### TUI Configuration Panel (v6.47.0+)

The interactive TUI (`dbbackup interactive`) includes a dedicated **CPU Optimization** panel where you can:

- View detected hardware (vendor, ISA features, cache, NUMA, governor)
- Toggle individual auto-tune subsystems on/off
- Override detected values (jobs, dump jobs, batch size, buffer KB, compression)
- Save settings to `.dbbackup.conf` or reset all to auto-detected defaults

## HugePages Integration (v6.17+)

On Linux systems with HugePages configured, dbbackup automatically detects and recommends optimal `shared_buffers` settings for PostgreSQL. HugePages reduce TLB misses and can provide a **30-50% improvement** in shared-buffer-heavy workloads.

**What dbbackup does:**

1. **Detects HugePages** -- reads `/proc/meminfo` for `HugePages_Total`, `HugePages_Free`, and `Hugepagesize`
2. **Recommends shared_buffers** -- calculates 75% of total HugePages memory as the optimal `shared_buffers` value
3. **Warns on Connect** -- if HugePages are available but PostgreSQL's `huge_pages` setting is `off`, a warning is logged
4. **Displays in TUI & CLI** -- the system profile view shows HugePages status, page count, and recommended settings

### Setting up HugePages (example for 1 GB shared_buffers)

```bash
# 1. Calculate pages needed: shared_buffers / 2MB + overhead
#    1 GB / 2 MB = 512 + ~88 overhead = 600
sudo sysctl -w vm.nr_hugepages=600

# Make persistent in /etc/sysctl.d/99-postgresql.conf
echo "vm.nr_hugepages = 600" >> /etc/sysctl.d/99-postgresql.conf

# 2. Allow the postgres group to use HugePages
PG_GID=$(id -g postgres)
echo "vm.hugetlb_shm_group = $PG_GID" >> /etc/sysctl.d/99-postgresql.conf
sudo sysctl -w vm.hugetlb_shm_group=$PG_GID

# 3. Set memlock limit for PostgreSQL (systemd overrides /etc/security/limits.conf)
mkdir -p /etc/systemd/system/postgresql@17-main.service.d
cat > /etc/systemd/system/postgresql@17-main.service.d/hugepages.conf << 'EOF'
[Service]
LimitMEMLOCK=infinity
EOF
sudo systemctl daemon-reload

# 4. Disable Transparent HugePages (causes latency spikes with PostgreSQL)
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
# Make persistent:
cat > /etc/tmpfiles.d/thp.conf << 'EOF'
w /sys/kernel/mm/transparent_hugepage/enabled - - - - madvise
w /sys/kernel/mm/transparent_hugepage/defrag - - - - defer+madvise
EOF

# 5. Enable in PostgreSQL and restart
sed -i 's/^#huge_pages = try/huge_pages = on/' /etc/postgresql/*/main/postgresql.conf
sudo systemctl restart postgresql

# 6. Verify HugePages are in use
grep HugePages /proc/meminfo
# HugePages_Rsvd should be > 0 (pages reserved by PostgreSQL)
```

> **Note:** If `nr_hugepages` allocates fewer pages than requested, drop caches first:
> `sync && echo 3 > /proc/sys/vm/drop_caches && sysctl -w vm.nr_hugepages=600`

**Scaling HugePages by shared_buffers size:**

| `shared_buffers` | `nr_hugepages` | Reserved Memory |
|-----------------|----------------|----------------|
| 256 MB | 200 | 400 MB |
| 1 GB | 600 | 1.2 GB |
| 4 GB | 2200 | 4.4 GB |
| 8 GB | 4200 | 8.4 GB |

**Verify with dbbackup:**

```bash
./dbbackup profile --dsn "postgres://user:pass@localhost/mydb"
# Look for the HugePages section in the output
```

## Linux Kernel Tuning (sysctl)

For database servers running dbbackup, tuning `sysctl` can significantly improve backup and restore throughput. Below is a production-tested example for a **32 GB / 16-core** PostgreSQL server:

```bash
# /etc/sysctl.d/99-postgresql.conf
# PostgreSQL / Database Server Tuning -- 32 GB RAM, 16 cores

# --- Memory / VM ---
vm.swappiness = 1                       # Minimize swap usage (or 0 if no swap)
vm.overcommit_memory = 2                # PostgreSQL recommended: don't overcommit
vm.overcommit_ratio = 80                # Allow up to 80% RAM commitment
vm.vfs_cache_pressure = 50              # Keep dentries/inodes cached longer

# Dirty page tuning -- smooth out large write bursts during backup/restore
vm.dirty_background_ratio = 5           # Start flushing at 5% dirty pages
vm.dirty_ratio = 40                     # Block writers at 40% dirty pages
vm.dirty_expire_centisecs = 3000        # Expire dirty pages after 30s
vm.dirty_writeback_centisecs = 500      # Flush daemon wakes every 5s

# --- Shared Memory (PostgreSQL) ---
kernel.shmmax = 26843545600             # 25 GB -- must be >= shared_buffers
kernel.shmall = 6553600                 # shmmax / PAGE_SIZE (4096)
kernel.sem = 250 32000 100 128          # Semaphores for PostgreSQL

# HugePages for PostgreSQL shared_buffers (see HugePages section above)
# vm.nr_hugepages = 600                 # shared_buffers / 2MB + overhead
# vm.hugetlb_shm_group = 104            # GID of postgres group

# --- Network (backup to SMB / NFS / cloud) ---
net.core.rmem_max = 16777216            # 16 MB receive buffer max
net.core.wmem_max = 16777216            # 16 MB send buffer max
net.core.rmem_default = 1048576         # 1 MB receive default
net.core.wmem_default = 1048576         # 1 MB send default
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216
net.ipv4.tcp_max_syn_backlog = 8192

# Faster connection recycling (helps dbbackup parallel workers)
net.ipv4.tcp_fin_timeout = 15           # Default 60 -- release sockets faster
net.ipv4.tcp_tw_reuse = 1               # Reuse TIME_WAIT for outgoing

# Shorter keepalive for database connections
net.ipv4.tcp_keepalive_time = 600       # Default 7200 -- detect dead peers in 10m
net.ipv4.tcp_keepalive_intvl = 30       # Probe interval
net.ipv4.tcp_keepalive_probes = 5       # Give up after 5 failed probes

# --- Filesystem ---
fs.file-max = 2097152                   # 2M open file handles
```

Apply with:

```bash
sudo sysctl --system
```

**Scaling guidelines:**

| RAM | `shmmax` | `overcommit_ratio` | `dirty_ratio` |
|-----|----------|--------------------|---------------|
| 8 GB | 6 GB | 80 | 30 |
| 16 GB | 12 GB | 80 | 35 |
| 32 GB | 25 GB | 80 | 40 |
| 64 GB+ | 50 GB | 80 | 40 |

> **Tip:** Pair these settings with HugePages (above) for maximum performance. On a 32 GB server with `shared_buffers=8GB`, set `vm.nr_hugepages=4200` (8 GB / 2 MB + ~100 overhead).
