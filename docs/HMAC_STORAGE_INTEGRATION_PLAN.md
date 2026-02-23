# HMAC File Server Integration Plan — dbbackup v7.0

> **Status**: Draft  
> **Date**: 2026-02-23  
> **Target Version**: dbbackup 7.0.0  
> **Depends On**: [hmac-file-server](https://github.com/PlusOne/hmac-file-server) v3.4.0+

---

## 1. Executive Summary

Add **hmac-file-server** as a first-class cloud storage backend in dbbackup, enabling
self-hosted backup storage over HTTPS on port 443 — without S3-compatible APIs,
cloud vendor lock-in, or firewall exceptions beyond standard HTTPS.

This is a strategic feature for **enterprise environments** where:
- Only port **443 (HTTPS)** is allowed through firewalls
- S3/Azure/GCS endpoints are blocked or unavailable
- Data sovereignty requires on-premise backup storage
- Developers need a zero-config backup target on internal infrastructure
- XMPP infrastructure already runs hmac-file-server

---

## 2. Architecture Overview

```
┌─────────────────┐         HTTPS (443)         ┌─────────────────────┐
│                 │  PUT /backups/v2/{hmac}/...  │                     │
│    dbbackup     │ ──────────────────────────► │  hmac-file-server   │
│    (client)     │                              │  (storage target)   │
│                 │ ◄────────────────────────── │                     │
│                 │  GET /backups/{path}          │  - HMAC auth        │
│                 │  GET /admin/files (list)      │  - Deduplication    │
│                 │  DELETE /admin/files/{id}     │  - Prometheus /metrics│
└─────────────────┘                              └─────────────────────┘
        │                                                 │
        │  Prometheus                          Prometheus  │
        ▼                                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Grafana Dashboard                           │
│         dbbackup panels  +  hmac-file-server panels             │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| API version | **V2 protocol** (`?v2=`) | Best balance of security (includes filesize in HMAC) and simplicity (no expiry management) |
| Auth for list/delete | **Admin Bearer token** | List and delete require admin API — HMAC only covers upload/download |
| File path convention | `/backups/{hostname}/{dbtype}/{filename}` | Mirrors existing cloud prefix pattern, enables multi-server storage |
| TLS | Via **reverse proxy** (nginx) on 443 | hmac-file-server doesn't do TLS natively; standard enterprise pattern |
| No new Go dependencies | **`net/http` + `crypto/hmac`** | Zero external deps — pure stdlib HTTP client |

---

## 3. HMAC Authentication Implementation

### 3.1 Upload (V2 Protocol)

```go
// computeHMACv2 generates the V2 HMAC token for upload authentication
func computeHMACv2(secret, filePath string, fileSize int64) string {
    // Format: HMAC-SHA256(secret, "{filePath}\x00{fileSize}")
    mac := hmac.New(sha256.New, []byte(secret))
    payload := filePath + "\x00" + strconv.FormatInt(fileSize, 10)
    mac.Write([]byte(payload))
    return hex.EncodeToString(mac.Sum(nil))
}

// Upload URL: PUT https://{endpoint}/{filePath}?v2={hmacToken}
// Body: raw file bytes
// Content-Length: required
// Response: 201 Created (success)
```

### 3.2 Download (Legacy GET — no auth on catch-all)

```go
// Download URL: GET https://{endpoint}/{filePath}
// No authentication needed for legacy GET downloads
// Response: 200 OK with file body, Content-Length, Content-Type headers
```

### 3.3 List Files (Admin API)

```go
// List URL: GET https://{endpoint}/admin/files?page=1&limit=1000
// Header: Authorization: Bearer {adminToken}
// Response: 200 OK with JSON array of file objects
```

### 3.4 Delete (Admin API)

```go
// Delete URL: DELETE https://{endpoint}/admin/files/{fileId}
// Header: Authorization: Bearer {adminToken}
// Response: 204 No Content
```

### 3.5 Security Considerations

- HMAC secret must match server's `[security] secret` config
- Admin token must match server's `[admin.auth] token` config  
- Both are stored in dbbackup config (masked in TUI, never logged)
- V2 HMAC includes file size → prevents replay with different payload
- All communication over TLS (443) via reverse proxy

---

## 4. Implementation Plan — File by File

### 4.1 New File: `internal/cloud/hmac.go`

**Purpose**: Implement `cloud.Backend` interface for hmac-file-server.

```go
package cloud

// HMACBackend implements Backend for hmac-file-server storage
type HMACBackend struct {
    client     *http.Client
    endpoint   string        // e.g., "https://backup.example.com"
    secret     string        // HMAC signing secret
    adminToken string        // Admin API bearer token (for list/delete)
    prefix     string        // Path prefix (e.g., "backups/myhost/postgres")
    config     *Config
}
```

**Interface methods**:

| Method | HTTP Call | Notes |
|--------|-----------|-------|
| `Upload()` | `PUT /{prefix}/{filename}?v2={hmac}` | Stream file body, report progress via `ProgressCallback` |
| `Download()` | `GET /{prefix}/{filename}` | Legacy GET (no auth), stream to local file |
| `List()` | `GET /admin/files?page=N&limit=100` | Paginate through admin API, filter by prefix |
| `Delete()` | `DELETE /admin/files/{id}` | Requires file ID from List response |
| `Exists()` | `HEAD /{prefix}/{filename}` | Check HTTP 200 vs 404 |
| `GetSize()` | `HEAD /{prefix}/{filename}` | Parse `Content-Length` header |
| `Name()` | returns `"hmac"` | |

**Estimated size**: ~350-400 lines  
**Dependencies**: `net/http`, `crypto/hmac`, `crypto/sha256`, `encoding/hex`, `encoding/json`  
**No external Go modules added.**

### 4.2 New File: `internal/cloud/hmac_test.go`

**Purpose**: Unit tests for HMAC token computation, URL construction, error handling.

**Test cases**:
- `TestComputeHMACv2` — verify token matches known-good values
- `TestComputeHMACv2_EmptySecret` — error handling
- `TestHMACBackend_Upload` — mock HTTP server, verify PUT request format
- `TestHMACBackend_Download` — mock HTTP server, verify GET request
- `TestHMACBackend_List` — mock admin API pagination
- `TestHMACBackend_Delete` — mock admin DELETE
- `TestHMACBackend_Exists` — HEAD request 200/404
- `TestHMACBackend_ContextCancellation` — verify Ctrl+C interrupts transfers
- `TestHMACBackend_RetryOnTransientError` — 503 → retry → succeed
- `TestHMACBackend_TLSSkipVerify` — self-signed cert support

**Estimated size**: ~300 lines

### 4.3 Modified File: `internal/cloud/interface.go`

**Changes**:
1. Add `"hmac"` case to `NewBackend()` provider routing:

```go
case "hmac", "hmac-file-server":
    return NewHMACBackend(cfg)
```

2. Extend `Config` struct with HMAC-specific fields:

```go
type Config struct {
    // ... existing fields ...
    
    // HMAC File Server specific
    HMACSecret     string // HMAC signing secret (shared with server)
    HMACAdminToken string // Admin API bearer token (for list/delete)
    HMACInsecure   bool   // Skip TLS certificate verification (self-signed certs)
}
```

3. Update `Validate()` to handle HMAC provider:

```go
if c.Provider == "hmac" || c.Provider == "hmac-file-server" {
    if c.Endpoint == "" {
        return fmt.Errorf("endpoint is required for hmac-file-server")
    }
    if c.HMACSecret == "" {
        return fmt.Errorf("hmac-secret is required for hmac-file-server")
    }
}
```

### 4.4 Modified File: `internal/config/config.go`

**Add fields to `Config` struct** (in the Cloud storage options section):

```go
// Cloud storage options (v2.0)
CloudEnabled    bool   
CloudProvider   string // "s3", "minio", "b2", "azure", "gcs", "hmac"  ← add "hmac"
CloudBucket     string 
CloudRegion     string 
CloudEndpoint   string 
CloudAccessKey  string 
CloudSecretKey  string 
CloudPrefix     string 
CloudAutoUpload bool   

// HMAC File Server storage (v7.0)
CloudHMACSecret     string // HMAC signing secret for hmac-file-server
CloudHMACAdminToken string // Admin API bearer token for list/delete operations
CloudHMACInsecure   bool   // Skip TLS verification (self-signed certificates)
```

**Update `New()` defaults** — no defaults needed (empty = unconfigured).

### 4.5 Modified File: `internal/config/persist.go`

**Add persistence** for new fields in config file `[cloud]` section:

```toml
[cloud]
provider = "hmac"
endpoint = "https://backup.internal.company.com"
prefix = "backups/dbserver01/postgres"
hmac_secret = "your-shared-secret"
hmac_admin_token = "your-admin-token"
hmac_insecure = false
auto_upload = true
```

### 4.6 Modified File: `cmd/cloud.go`

**Add CLI flags** for HMAC-specific options:

```go
// In init() — register on all cloud subcommands
for _, cmd := range []*cobra.Command{cloudUploadCmd, cloudDownloadCmd, ...} {
    cmd.Flags().StringVar(&hmacSecret, "hmac-secret",
        getEnv("DBBACKUP_HMAC_SECRET", ""),
        "HMAC signing secret for hmac-file-server")
    cmd.Flags().StringVar(&hmacAdminToken, "hmac-admin-token",
        getEnv("DBBACKUP_HMAC_ADMIN_TOKEN", ""),
        "Admin API token for hmac-file-server (list/delete)")
    cmd.Flags().BoolVar(&hmacInsecure, "hmac-insecure", false,
        "Skip TLS certificate verification (self-signed certs)")
}
```

**Update `getCloudBackend()`** to pass HMAC fields:

```go
cfg := &cloud.Config{
    // ... existing ...
    HMACSecret:     hmacSecret,
    HMACAdminToken: hmacAdminToken,
    HMACInsecure:   hmacInsecure,
}
```

**New CLI flags summary**:

| Flag | Env Var | Type | Default | Description |
|------|---------|------|---------|-------------|
| `--hmac-secret` | `DBBACKUP_HMAC_SECRET` | string | `""` | HMAC signing secret |
| `--hmac-admin-token` | `DBBACKUP_HMAC_ADMIN_TOKEN` | string | `""` | Admin bearer token |
| `--hmac-insecure` | — | bool | `false` | Skip TLS verify |

### 4.7 Modified File: `cmd/backup.go`

**Update cloud URI parsing** to support `hmac://` URIs:

```
dbbackup backup single mydb --cloud hmac://backup.example.com/backups/myhost
```

This maps to:
- `CloudProvider = "hmac"`
- `CloudEndpoint = "https://backup.example.com"` (default HTTPS)
- `CloudPrefix = "backups/myhost"`

### 4.8 Modified File: `cmd/cloud_status.go`

**Add HMAC-specific status checks**:

```go
case "hmac", "hmac-file-server":
    // 1. Check endpoint reachability (GET /health)
    // 2. Check HMAC auth (PUT small test file, verify 201)
    // 3. Check admin API (GET /admin/stats, verify 200)
    // 4. Report server version, storage usage, file count
```

### 4.9 Modified File: `internal/tui/settings.go`

**Extend the "Advanced & Cloud Settings" section** with HMAC provider fields:

Current provider cycling: `S3 → MinIO → B2 → Azure → GCS`  
New cycling: `S3 → MinIO → B2 → Azure → GCS → HMAC`

**Add conditional fields** when provider is `hmac`:

| # | Field | Type | Display |
|---|-------|------|---------|
| 1 | `cloud_endpoint` | string | Full URL (e.g., `https://backup.example.com`) |
| 2 | `cloud_hmac_secret` | string | Masked: `***...{last4}` |
| 3 | `cloud_hmac_admin_token` | string | Masked: `********` |
| 4 | `cloud_prefix` | string | Path prefix |
| 5 | `cloud_hmac_insecure` | bool | Toggle |
| 6 | `cloud_auto_upload` | bool | Toggle |

**Hide irrelevant fields** when HMAC is selected:
- `cloud_bucket` — not used (path-based, not bucket-based)
- `cloud_region` — not used
- `cloud_access_key` — replaced by `hmac_secret`
- `cloud_secret_key` — replaced by `hmac_admin_token`

**Settings summary line** when HMAC is active:
```
Cloud: hmac (backup.example.com) [auto-upload]
```

### 4.10 Modified File: `cmd/cloud_sync.go`

**No changes required** — sync uses `cloud.Backend` interface generically.
The HMAC backend implements the same interface, so sync works automatically.

### 4.11 Modified File: `cmd/cross_region_sync.go`

**Add HMAC as valid source/destination** — supports `--source-provider hmac`
and `--dest-provider hmac` with corresponding secret/token flags.

### 4.12 Testing Files

| File | Purpose |
|------|---------|
| `internal/cloud/hmac_test.go` | Unit tests (mock HTTP server) |
| `tests/hmac_integration_test.go` | Integration test against real hmac-file-server (Docker) |

### 4.13 Docker Compose: `docker-compose.hmac.yml`

For local development and CI testing:

```yaml
services:
  hmac-file-server:
    image: ghcr.io/plusone/hmac-file-server:3.4.0
    ports:
      - "8080:8080"
    volumes:
      - ./tests/hmac-config.toml:/config/config.toml
      - hmac-uploads:/uploads
    environment:
      - ADMIN_TOKEN=test-admin-token

volumes:
  hmac-uploads:
```

---

## 5. Config Struct Changes — Complete Diff

### `internal/config/config.go` additions

```go
// Cloud storage options (v2.0+)
CloudEnabled    bool   
CloudProvider   string // "s3", "minio", "b2", "azure", "gcs", "hmac"
CloudBucket     string 
CloudRegion     string 
CloudEndpoint   string 
CloudAccessKey  string 
CloudSecretKey  string 
CloudPrefix     string 
CloudAutoUpload bool   

// HMAC File Server options (v7.0) — used when CloudProvider == "hmac"
CloudHMACSecret     string // Shared HMAC signing secret
CloudHMACAdminToken string // Admin API bearer token for list/delete
CloudHMACInsecure   bool   // Accept self-signed TLS certificates
```

### `internal/cloud/interface.go` Config additions

```go
type Config struct {
    Provider       string
    Bucket         string    // Not used for HMAC provider
    Region         string    // Not used for HMAC provider
    Endpoint       string    // Required: full URL (https://backup.example.com)
    AccessKey      string    // Not used for HMAC provider
    SecretKey      string    // Not used for HMAC provider
    UseSSL         bool
    PathStyle      bool
    Prefix         string    // Path prefix (e.g., "backups/myhost/postgres")
    Timeout        int
    MaxRetries     int
    Concurrency    int       // Parallel upload/download streams
    BandwidthLimit int64

    // S3 Object Lock
    ObjectLockEnabled bool
    ObjectLockMode    string
    ObjectLockDays    int

    // HMAC File Server (v7.0)
    HMACSecret     string   // HMAC signing secret
    HMACAdminToken string   // Admin bearer token
    HMACInsecure   bool     // Skip TLS certificate verification
}
```

---

## 6. Enterprise Port 443 Deployment

### The Problem

Enterprise networks typically:
- Block all outbound ports except 80 and 443
- Block S3 endpoints (`*.amazonaws.com`) via proxy/firewall rules
- Require all traffic through an HTTPS proxy
- Need on-premise data residency

### The Solution

hmac-file-server behind nginx on port 443:

```
┌──────────┐     443/TLS      ┌──────────┐      8080       ┌──────────────────┐
│ dbbackup │ ───────────────► │  nginx   │ ──────────────► │ hmac-file-server │
│ (client) │                  │ (TLS)    │                  │ (storage)        │
└──────────┘                  └──────────┘                  └──────────────────┘
```

### Nginx Configuration (Enterprise)

```nginx
server {
    listen 443 ssl http2;
    server_name backup.internal.company.com;

    ssl_certificate     /etc/ssl/certs/backup.crt;
    ssl_certificate_key /etc/ssl/private/backup.key;

    # Enterprise: large backups
    client_max_body_size 50G;
    proxy_request_buffering off;    # Stream directly, don't buffer to disk
    proxy_read_timeout 3600s;       # 1 hour for large uploads
    proxy_send_timeout 3600s;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### dbbackup Client Configuration

```toml
[cloud]
provider = "hmac"
endpoint = "https://backup.internal.company.com"
prefix = "backups/dbserver01/postgres"
hmac_secret = "shared-secret-from-server-config"
hmac_admin_token = "admin-token-from-server-config"
auto_upload = true
```

### CLI Usage

```bash
# One-time backup with cloud upload
dbbackup backup single mydb \
  --cloud-provider hmac \
  --cloud-endpoint https://backup.internal.company.com \
  --hmac-secret "$DBBACKUP_HMAC_SECRET" \
  --cloud-auto-upload

# Cloud URI shorthand
dbbackup backup single mydb --cloud hmac://backup.internal.company.com/backups/myhost

# Check connectivity
dbbackup cloud status \
  --cloud-provider hmac \
  --cloud-endpoint https://backup.internal.company.com \
  --hmac-secret "$DBBACKUP_HMAC_SECRET" \
  --hmac-admin-token "$DBBACKUP_HMAC_ADMIN_TOKEN"

# List remote backups
dbbackup cloud list --cloud-provider hmac --cloud-endpoint https://backup.internal.company.com

# Sync local backups to HMAC server
dbbackup cloud sync ./pg_backups/ \
  --cloud-provider hmac \
  --cloud-endpoint https://backup.internal.company.com

# Cross-region: S3 → HMAC migration
dbbackup cross-region-sync \
  --source-provider s3 --source-bucket my-backups --source-region us-east-1 \
  --dest-provider hmac --dest-endpoint https://backup.eu.company.com
```

### Environment Variables

```bash
export DBBACKUP_CLOUD_PROVIDER=hmac
export DBBACKUP_CLOUD_ENDPOINT=https://backup.internal.company.com
export DBBACKUP_CLOUD_PREFIX=backups/dbserver01/postgres
export DBBACKUP_HMAC_SECRET=your-shared-secret
export DBBACKUP_HMAC_ADMIN_TOKEN=your-admin-token
```

---

## 7. TUI Integration — Detailed Design

### 7.1 Settings Screen Provider Selector

Current cycling order: `S3 → MinIO → B2 → Azure → GCS`  
New: `S3 → MinIO → B2 → Azure → GCS → HMAC File Server`

When user selects "HMAC File Server", the settings section dynamically changes:

```
┌─────────────────────────────────────────────────┐
│  ☁️  Cloud Storage Settings                      │
│                                                  │
│  Provider:      [HMAC File Server]    ← toggle   │
│  Endpoint:      https://backup.example.com       │
│  Path Prefix:   backups/dbserver01/postgres       │
│  HMAC Secret:   ***...a1b2                        │
│  Admin Token:   ********                          │
│  Skip TLS:      No                                │
│  Auto-Upload:   Yes                               │
│                                                  │
│  Status: ✅ Connected (147 files, 23.4 GiB)      │
│                                                  │
│  [Tab] Next  [Enter] Edit  [Esc] Back            │
└─────────────────────────────────────────────────┘
```

### 7.2 Status Line

When HMAC is configured and connected, the TUI status bar shows:
```
Cloud: hmac (backup.example.com) [auto-upload] — 147 files, 23.4 GiB
```

### 7.3 Field Visibility Rules

| Field | S3/MinIO/B2 | Azure | GCS | HMAC |
|-------|:-----------:|:-----:|:---:|:----:|
| Bucket/Container | ✅ | ✅ | ✅ | ❌ |
| Region | ✅ | ❌ | ✅ | ❌ |
| Access Key | ✅ | ✅ | ✅ | ❌ |
| Secret Key | ✅ | ✅ | ❌ | ❌ |
| Endpoint | MinIO/B2 | ❌ | ❌ | ✅ (required) |
| HMAC Secret | ❌ | ❌ | ❌ | ✅ |
| Admin Token | ❌ | ❌ | ❌ | ✅ |
| Skip TLS Verify | ❌ | ❌ | ❌ | ✅ |
| Prefix | ✅ | ✅ | ✅ | ✅ |
| Auto-Upload | ✅ | ✅ | ✅ | ✅ |

---

## 8. Grafana Dashboard Extension

### New Row: "HMAC File Server Storage"

Add panels sourced from hmac-file-server's `/metrics` Prometheus endpoint:

| Panel | Metric | Type |
|-------|--------|------|
| Upload Rate | `hmac_uploads_total` | Counter → rate() |
| Download Rate | `hmac_downloads_total` | Counter → rate() |
| Storage Used | `hmac_storage_bytes` | Gauge |
| File Count | `hmac_files_total` | Gauge |
| Upload Errors | `hmac_upload_errors_total` | Counter |
| Auth Failures | `hmac_auth_failures_total` | Counter |
| Request Latency | `hmac_request_duration_seconds` | Histogram |
| Dedup Ratio | `hmac_dedup_hits_total / hmac_uploads_total` | Calculated |

These panels only appear when hmac-file-server Prometheus data source is configured.

---

## 9. HTTP Client Implementation Details

### 9.1 Connection Pooling

```go
transport := &http.Transport{
    MaxIdleConns:        10,
    MaxIdleConnsPerHost: 10,
    IdleConnTimeout:     90 * time.Second,
    TLSClientConfig: &tls.Config{
        InsecureSkipVerify: cfg.HMACInsecure,
    },
}
client := &http.Client{
    Transport: transport,
    Timeout:   time.Duration(cfg.Timeout) * time.Second,
}
```

### 9.2 Upload with Progress Tracking

```go
func (h *HMACBackend) Upload(ctx context.Context, localPath, remotePath string, progress ProgressCallback) error {
    file, _ := os.Open(localPath)
    defer file.Close()
    stat, _ := file.Stat()
    
    // Compute HMAC
    fullPath := h.prefix + "/" + remotePath
    token := computeHMACv2(h.secret, fullPath, stat.Size())
    
    // Build URL
    url := fmt.Sprintf("%s/%s?v2=%s", h.endpoint, fullPath, token)
    
    // Wrap reader for progress
    reader := NewProgressReader(file, stat.Size(), progress)
    
    req, _ := http.NewRequestWithContext(ctx, "PUT", url, reader)
    req.ContentLength = stat.Size()
    req.Header.Set("Content-Type", "application/octet-stream")
    
    resp, err := h.client.Do(req)
    // expect 200 or 201
}
```

### 9.3 Retry Logic

Reuse existing `internal/cloud/retry.go` — the HMAC backend returns standard HTTP
errors that the retry wrapper can handle:

- `429 Too Many Requests` → retry after `Retry-After` header
- `503 Service Unavailable` → retry with exponential backoff
- `5xx` → retry up to `MaxRetries`
- `401/403` → fail immediately (auth error, no retry)

### 9.4 Bandwidth Throttling

Reuse existing `internal/cloud/throttle.go` — wrap the upload `io.Reader` with the
throttled reader. Same pattern as S3/Azure backends.

---

## 10. Migration Path from S3/Azure/GCS

For users migrating from cloud to self-hosted HMAC:

```bash
# Cross-region sync from S3 to HMAC
dbbackup cross-region-sync \
  --source-provider s3 \
  --source-bucket my-old-backups \
  --source-region us-east-1 \
  --dest-provider hmac \
  --dest-endpoint https://backup.company.com \
  --parallel 4
```

This uses the existing `cross-region-sync` command — no new code needed beyond
the HMAC backend implementation.

---

## 11. Security Analysis

### Threats & Mitigations

| Threat | Mitigation |
|--------|-----------|
| HMAC secret exposure in logs | Never log secret; mask in TUI (`***...{last4}`) |
| Admin token in config file | File permissions 0600; warn if world-readable |
| Man-in-the-middle | TLS via nginx (port 443); `--hmac-insecure` only for dev |
| Replay attack | V2 HMAC includes file size; different file = different token |
| Unauthorized delete | Delete requires admin token (separate from HMAC secret) |
| Brute-force HMAC | Server rate limits (429); configurable per-IP/JID |
| Config file secrets | Use env vars (`DBBACKUP_HMAC_SECRET`) in production |

### Secret Handling

```go
// Config persistence: mask secrets in config file comments
// Store actual values, but show masked in TUI and logs
func maskSecret(s string) string {
    if len(s) <= 4 {
        return "****"
    }
    return "***..." + s[len(s)-4:]
}
```

---

## 12. Implementation Phases

### Phase 1: Core Backend (v7.0-alpha)

| Task | Files | Est. Lines | Effort |
|------|-------|-----------|--------|
| `HMACBackend` struct + all interface methods | `internal/cloud/hmac.go` | ~400 | 1 day |
| HMAC token computation (V2) | `internal/cloud/hmac.go` | ~30 | 1 hour |
| Provider routing (`NewBackend`) | `internal/cloud/interface.go` | ~10 | 30 min |
| Config struct fields | `internal/config/config.go` | ~5 | 15 min |
| Config persistence | `internal/config/persist.go` | ~15 | 30 min |
| CLI flags | `cmd/cloud.go` | ~20 | 30 min |
| Unit tests | `internal/cloud/hmac_test.go` | ~300 | 1 day |
| **Subtotal** | | **~780** | **~3 days** |

### Phase 2: CLI & Status (v7.0-beta)

| Task | Files | Est. Lines | Effort |
|------|-------|-----------|--------|
| Cloud status HMAC checks | `cmd/cloud_status.go` | ~80 | 2 hours |
| Cloud URI parsing (`hmac://`) | `cmd/backup.go`, `internal/cloud/uri.go` | ~30 | 1 hour |
| Cross-region sync HMAC flags | `cmd/cross_region_sync.go` | ~20 | 30 min |
| Docker compose test env | `docker-compose.hmac.yml` | ~25 | 30 min |
| Integration test | `tests/hmac_integration_test.go` | ~150 | 4 hours |
| **Subtotal** | | **~305** | **~1 day** |

### Phase 3: TUI & Polish (v7.0-rc)

| Task | Files | Est. Lines | Effort |
|------|-------|-----------|--------|
| Settings screen HMAC fields | `internal/tui/settings.go` | ~120 | 4 hours |
| Field visibility logic | `internal/tui/settings.go` | ~60 | 2 hours |
| Status line update | `internal/tui/settings.go` | ~15 | 30 min |
| Grafana dashboard panels | `grafana/dbbackup-dashboard.json` | ~200 | 2 hours |
| Documentation | `docs/HMAC_STORAGE.md` | ~200 | 2 hours |
| CHANGELOG entry | `CHANGELOG.md` | ~30 | 15 min |
| **Subtotal** | | **~625** | **~1.5 days** |

### Total Estimate

| Phase | Lines | Effort |
|-------|-------|--------|
| Phase 1: Core Backend | ~780 | 3 days |
| Phase 2: CLI & Status | ~305 | 1 day |
| Phase 3: TUI & Polish | ~625 | 1.5 days |
| **Total** | **~1,710** | **~5.5 days** |

---

## 13. Files Changed/Created Summary

### New Files (4)

| File | Purpose |
|------|---------|
| `internal/cloud/hmac.go` | HMAC backend implementation |
| `internal/cloud/hmac_test.go` | Unit tests |
| `docker-compose.hmac.yml` | Test environment |
| `docs/HMAC_STORAGE.md` | User documentation |

### Modified Files (9)

| File | Change |
|------|--------|
| `internal/cloud/interface.go` | Add `HMACBackend` to `NewBackend()`, extend `Config` struct |
| `internal/config/config.go` | Add `CloudHMAC*` fields to `Config` struct |
| `internal/config/persist.go` | Persist/load HMAC config fields |
| `cmd/cloud.go` | Add `--hmac-*` CLI flags, update `getCloudBackend()` |
| `cmd/cloud_status.go` | Add HMAC health/connectivity checks |
| `cmd/cloud_sync.go` | Add `--hmac-*` flags (same pattern as cloud.go) |
| `cmd/cross_region_sync.go` | Add `--source-hmac-*` / `--dest-hmac-*` flags |
| `cmd/backup.go` | Support `hmac://` cloud URI scheme |
| `internal/tui/settings.go` | HMAC provider in cycling selector, conditional fields |
| `grafana/dbbackup-dashboard.json` | HMAC File Server monitoring row |

---

## 14. Compatibility & Breaking Changes

- **No breaking changes** — purely additive
- Existing cloud providers (`s3`, `minio`, `b2`, `azure`, `gcs`) unaffected
- Config file backward compatible (new fields optional, default to empty)
- CLI backward compatible (new flags have empty defaults)
- TUI backward compatible (HMAC only shows when selected)
- Minimum hmac-file-server version: **v3.4.0** (admin API required for list/delete)

---

## 15. Why This Is a v7.0 Feature

| Aspect | Impact |
|--------|--------|
| **New storage paradigm** | First non-object-storage backend — HTTP file server with HMAC auth |
| **Enterprise unlock** | Port 443 support eliminates the #1 enterprise blocker for cloud backups |
| **Zero vendor lock-in** | Self-hosted, open-source storage with no cloud subscription |
| **Cross-project synergy** | Tight integration between two PlusOne projects |
| **New config surface** | 3 new config fields, new TUI section, new CLI flags |
| **Complete feature** | Upload, download, list, delete, sync, status, TUI, Grafana |

This is a complete, self-contained feature that warrants a major version bump and
positions dbbackup as the first database backup tool with native self-hosted
HTTPS storage integration.
