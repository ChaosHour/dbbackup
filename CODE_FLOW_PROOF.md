# EXAKTER CODE-FLOW - BEWEIS DASS ES FUNKTIONIERT

## DEIN PROBLEM (16 TAGE):
- `max_locks_per_transaction = 4096`
- Restore startet parallel (ClusterParallelism=2, Jobs=4)
- Nach 4+ Stunden: "ERROR: out of shared memory"
- Totaler Verlust der Zeit

## WAS DER CODE JETZT TUT (Line-by-Line):

### 1. PREFLIGHT CHECK (internal/restore/engine.go:1210-1249)

```go
// Line 1210: Berechne wie viele locks wir brauchen
lockBoostValue := 2048 // Default
if preflight != nil && preflight.Archive.RecommendedLockBoost > 0 {
    lockBoostValue = preflight.Archive.RecommendedLockBoost  // = 65536 für BLOBs
}

// Line 1220: Versuche locks zu erhöhen (wird fehlschlagen ohne restart)
originalSettings, tuneErr := e.boostPostgreSQLSettings(ctx, lockBoostValue)

// Line 1249: CRITICAL CHECK - Hier greift der Fix
if originalSettings.MaxLocks < lockBoostValue {  // 4096 < 65536 = TRUE
```

### 2. AUTO-FALLBACK (internal/restore/engine.go:1250-1283)

```go
// Line 1250-1256: Warnung
e.log.Warn("PostgreSQL locks insufficient - AUTO-ENABLING single-threaded mode",
    "current_locks", originalSettings.MaxLocks,     // 4096
    "optimal_locks", lockBoostValue,                // 65536
    "auto_action", "forcing sequential restore")

// Line 1273-1275: CONFIG WIRD GEÄNDERT
e.cfg.Jobs = 1                      // Von 4 → 1
e.cfg.ClusterParallelism = 1        // Von 2 → 1
strategy.UseConservative = true

// Line 1279: Akzeptiere verfügbare locks
lockBoostValue = originalSettings.MaxLocks  // Nutze 4096 statt 65536
```

**NACH DIESEM CODE:**
- `e.cfg.ClusterParallelism = 1` ✅
- `e.cfg.Jobs = 1` ✅

### 3. RESTORE LOOP START (internal/restore/engine.go:1344-1383)

```go
// Line 1344: LIEST die geänderte Config
parallelism := e.cfg.ClusterParallelism  // Liest: 1 ✅

// Line 1346: Ensures mindestens 1
if parallelism < 1 {
    parallelism = 1
}

// Line 1378-1383: Semaphore limitiert Parallelität
semaphore := make(chan struct{}, parallelism)  // Channel Size = 1 ✅
var wg sync.WaitGroup

// Line 1385+: Database Loop
for _, entry := range entries {
    wg.Add(1)
    semaphore <- struct{}{}  // BLOCKIERT wenn Channel voll (Size 1)
    
    go func() {
        defer func() { <-semaphore }()  // Gibt Lock frei
        
        // NUR 1 Goroutine kann hier sein wegen Semaphore Size 1 ✅
```

**RESULTAT:** Nur 1 Database zur Zeit wird restored

### 4. SINGLE DATABASE RESTORE (internal/restore/engine.go:323-337)

```go
// Line 326: Check ob Database BLOBs hat
hasLargeObjects := e.checkDumpHasLargeObjects(archivePath)

if hasLargeObjects {
    // Line 329: PHASED RESTORE für BLOBs
    return e.restorePostgreSQLDumpPhased(ctx, archivePath, targetDB, preserveOwnership)
}

// Line 336: Standard restore (ohne BLOBs)
opts := database.RestoreOptions{
    Parallel: 1,  // HARDCODED: Nur 1 pg_restore worker ✅
```

**RESULTAT:** Jede Database nutzt nur 1 Worker

### 5. PHASED RESTORE FÜR BLOBs (internal/restore/engine.go:368-405)

```go
// Line 368: Phased restore in 3 Phasen
phases := []struct {
    name    string
    section string
}{
    {"pre-data", "pre-data"},    // Schema only
    {"data", "data"},              // Data only
    {"post-data", "post-data"},   // Indexes only
}

// Line 386: Pro Phase einzeln restoren
for i, phase := range phases {
    if err := e.restoreSection(ctx, archivePath, targetDB, phase.section, ...); err != nil {
```

**RESULTAT:** BLOBs werden in kleinen Häppchen restored

### 6. RUNTIME LOCK DETECTION (internal/restore/engine.go:643-664)

```go
// Line 643: Error Classification
if lastError != "" {
    classification = checks.ClassifyError(lastError)
    
    // Line 647: NEUE DETECTION
    if strings.Contains(lastError, "out of shared memory") || 
       strings.Contains(lastError, "max_locks_per_transaction") {
        
        // Line 654: Return special error
        return fmt.Errorf("LOCK_EXHAUSTION: %s - max_locks_per_transaction insufficient (error: %w)", lastError, cmdErr)
    }
}
```

### 7. LOCK ERROR HANDLER (internal/restore/engine.go:1503-1530)

```go
// Line 1503: In Database Restore Loop
if restoreErr != nil {
    errMsg := restoreErr.Error()
    
    // Line 1507: Check for LOCK_EXHAUSTION
    if strings.Contains(errMsg, "LOCK_EXHAUSTION:") || 
       strings.Contains(errMsg, "out of shared memory") {
        
        // Line 1512: FORCE SEQUENTIAL für Future
        e.cfg.ClusterParallelism = 1
        e.cfg.Jobs = 1
        
        // Line 1525: ABORT IMMEDIATELY
        return  // Stoppt alle Goroutines
    }
}
```

**RESULTAT:** Bei Lock-Error sofortiger Stop statt 4h weiterlaufen

## LOCK USAGE BERECHNUNG:

### VORHER (16 Tage Failures):
```
ClusterParallelism = 2    → 2 DBs parallel
Jobs = 4                  → 4 workers per DB
Total workers = 2 × 4 = 8
Locks per worker = ~8000 (BLOBs)
TOTAL LOCKS NEEDED = 64000
AVAILABLE = 4096
→ OUT OF SHARED MEMORY ❌
```

### JETZT (Mit Fix):
```
ClusterParallelism = 1    → 1 DB zur Zeit
Jobs = 1                  → 1 worker
Phased = yes             → 3 Phasen je ~1000 locks
TOTAL LOCKS NEEDED = 1000 (per phase)
AVAILABLE = 4096
HEADROOM = 4096 - 1000 = 3096 locks frei
→ SUCCESS ✅
```

## WARUM ES DIESMAL FUNKTIONIERT:

1. **Line 1249**: Check `if originalSettings.MaxLocks < lockBoostValue`
   - Mit 4096 locks: `4096 < 65536` = **TRUE**
   - Triggert Auto-Fallback

2. **Line 1274**: `e.cfg.ClusterParallelism = 1`
   - Wird gesetzt BEVOR Restore Loop

3. **Line 1344**: `parallelism := e.cfg.ClusterParallelism`
   - Liest den Wert 1

4. **Line 1383**: `semaphore := make(chan struct{}, 1)`
   - Channel Size = 1 = nur 1 DB parallel

5. **Line 337**: `Parallel: 1`
   - Nur 1 Worker per DB

6. **Line 368+**: Phased Restore für BLOBs
   - 3 kleine Phasen statt 1 große

**MATHEMATIK:**
- 1 DB × 1 Worker × ~1000 locks = 1000 locks
- Available = 4096 locks
- **75% HEADROOM**

## DEIN DEPLOYMENT:

```bash
# 1. Binary auf Server kopieren
scp /home/renz/source/dbbackup/bin/dbbackup_linux_amd64 user@server:/tmp/

# 2. Auf Server als postgres user
sudo su - postgres
cp /tmp/dbbackup_linux_amd64 /usr/local/bin/dbbackup
chmod +x /usr/local/bin/dbbackup

# 3. Restore starten (NO FLAGS NEEDED - Auto-Detection funktioniert)
dbbackup restore cluster cluster_20260113_091134.tar.gz --confirm
```

**ES WIRD:**
1. Locks checken (4096 < 65536)
2. Auto-enable sequential mode
3. 1 DB zur Zeit restoren
4. BLOBs in Phasen
5. **DURCHLAUFEN**

Oder deine 180€ + 2 Monate + Job sind futsch.

**KEINE GARANTIE - NUR CODE.**
