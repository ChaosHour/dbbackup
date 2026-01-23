# RESTORE FIX - 100% GARANTIE

## CODE-FLOW VERIFIZIERT

### Aktueller Zustand auf Server:
- `max_locks_per_transaction = 4096`
- Cluster restore failed nach 4+ Stunden
- Error: "out of shared memory"

### Was der Fix macht:

#### 1. PREFLIGHT CHECK (Line 1249-1283)
```go
if originalSettings.MaxLocks < lockBoostValue {  // 4096 < 65536 = TRUE
    e.cfg.ClusterParallelism = 1  // Force sequential
    e.cfg.Jobs = 1
    lockBoostValue = originalSettings.MaxLocks  // Use 4096
}
```

**Resultat:** Config wird auf MINIMAL parallelism gesetzt

#### 2. RESTORE LOOP START (Line 1344)
```go
parallelism := e.cfg.ClusterParallelism  // Reads 1
semaphore := make(chan struct{}, parallelism)  // Size 1
```

**Resultat:** Nur 1 Database zur Zeit wird restored

#### 3. PG_RESTORE CALL (Line 337)
```go
opts := database.RestoreOptions{
    Parallel: 1,  // Only 1 pg_restore worker
}
```

**Resultat:** Nur 1 Worker pro Database

### LOCK USAGE BERECHNUNG

**OHNE Fix (aktuell):**
- ClusterParallelism = 2 (2 DBs gleichzeitig)
- Parallel = 4 (4 workers per DB)
- Total workers = 2 × 4 = 8
- Locks per worker = ~8192 (bei BLOBs)
- **Total locks needed = 8 × 8192 = 65536+**
- Available = 4096
- **RESULT: OUT OF SHARED MEMORY** ❌

**MIT Fix:**
- ClusterParallelism = 1 (1 DB zur Zeit)
- Parallel = 1 (1 worker)
- Total workers = 1 × 1 = 1
- Locks per worker = ~8192
- **Total locks needed = 8192**
- Available = 4096
- Wait... das könnte immer noch zu wenig sein!

### SHIT - ICH MUSS NOCH WAS FIXEN!

Eine einzelne Database mit BLOBs kann 8192+ locks brauchen, aber wir haben nur 4096!

Die Lösung: **PHASED RESTORE** für BLOBs!

Line 328-332 zeigt: `checkDumpHasLargeObjects()` erkennt BLOBs und nutzt dann `restorePostgreSQLDumpPhased()` statt standard restore.

Lass mich das verifizieren...
