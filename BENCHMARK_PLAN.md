# DBBackup Benchmark — 4-Tage-Plan

## Situation (21.02.2026)

| Storage | Gerät | Gesamt | Frei | Verwendung |
|---------|-------|--------|------|------------|
| Lokale SSD | `/dev/sda1` → `/` | 301 GB | ~276 GB | **DB-Daten (Docker)** |
| HC-Volume | `/dev/sdb` → `/mnt/HC_Volume_104620695` | 300 GB | ~297 GB | **Lokale Backups** |
| StorageBox | SMB → `/mnt/smb-devdb` | 10 TB | ~9.7 TB | **Netzwerk-Backups** |

**Was passiert ist:** `qa_wal_archive` hatte 298 GB WAL-Logs angesammelt (archive_mode=on,
kein Limit). Server war nicht mehr erreichbar → `/var/lib/postgresql` gelöscht.
→ **Fix:** WAL-Archivierung ist in allen Benchmark-Containern **dauerhaft deaktiviert**.

---

## Script-Referenz

```bash
# Neues Script (sauber, kein WAL-Archiving):
./dbbackup-benchmark.sh --help

# Docker-Compose:
docker-compose.benchmark.yml
```

### Wichtige Flags

| Flag | Beschreibung | Default |
|------|-------------|---------|
| `--size GB` | DB-Größe in GB | 10 |
| `--engines LIST` | postgres,mariadb,mysql | alle |
| `--smb` | Zusätzlich gegen SMB-StorageBox testen | nein |
| `--runs N` | Backup-Wiederholungen je Target | 1 |
| `--dry-run` | Nur Checks, nichts ausführen | nein |

### Ergebnisse landen in:
```
/mnt/HC_Volume_104620695/benchmarks/DATUM/
  results.json    ← alle Messwerte strukturiert
  results.csv     ← für Excel/Sheets
  postgres_*.log  ← Backup-Logs
  mariadb_*.log
  mysql_*.log
```

---

## Tag 1 — Heute (21.02.2026) — Verifikation 10 GB

**Ziel:** Sicherstellen dass alles läuft, bevor wir 100 GB verschwenden.

### Speicher-Matrix für diesen Tag

```
Postgres  10 GB → Docker-Volume auf /dev/sda1  (braucht: 10 GB)
Backup    ×1    → /mnt/HC_Volume_104620695      (braucht: max 5 GB komprimiert)
Cleanup   nach jeder Messung                    (nichts bleibt liegen)

Benötigt gesamt: ~30 GB (DB ist 3× im Lifecycle: create + dump in-flight + backup)
Verfügbar:       276 GB (sda1) + 297 GB (sdb) → mehr als genug
```

### Befehle

```bash
cd /root/dbbackup

# 1. Alle 3 Engines, 10 GB, nur lokaler Storage
./dbbackup-benchmark.sh --size 10 --engines postgres,mariadb,mysql

# 2. Ergebnis prüfen
cat /mnt/HC_Volume_104620695/benchmarks/$(ls -t /mnt/HC_Volume_104620695/benchmarks/ | head -1)/results.csv
```

**Erwartete Laufzeit Tag 1:** ~2-3 Stunden (3× DB anlegen + 3× Backup)

---

## Tag 2 — 22.02.2026 — 50 GB Benchmark

**Ziel:** Realistischere Messwerte, erste Kompressionsraten-Analyse.

### Speicher-Matrix für Tag 2

```
DB-Größe:  50 GB je Engine  (sequenziell, nie gleichzeitig)
Backup:    komprimiert ≈ 15-25 GB je Engine
Disk-Bedarf je Engine: 50 GB (DB) + 25 GB (Backup) = 75 GB
Cleanup danach → nächste Engine startet auf bereinigtem System
```

### Befehle

```bash
cd /root/dbbackup

# Lokaler Benchmark + SMB-Vergleich
./dbbackup-benchmark.sh \
    --size 50 \
    --engines postgres,mariadb,mysql \
    --smb \
    --runs 2

# Bei PostgreSQL-Problemen: nur PG zuerst
./dbbackup-benchmark.sh --size 50 --engines postgres
```

**Erwartete Laufzeit Tag 2:** ~4-6 Stunden

---

## Tag 3 — 23.02.2026 — 100 GB Benchmark

**Ziel:** Vollständiger Benchmark, alle Storages, Reproduzierbarkeit.

### Speicher-Matrix für Tag 3

```
DB-Größe:    100 GB je Engine
Backup lokal: ~30-50 GB (Level-6 Kompression, sofort löschen nach Messung)
Backup SMB:   ~30-50 GB (über Netzwerk, sofort löschen)

Disk-Check vorher:
  / (sda1)           braucht: 130 GB  → verfügbar: jetzt ~276 GB ✓
  HC-Volume (sdb)    braucht: 70 GB   → verfügbar: ~297 GB ✓
  SMB                braucht: 70 GB   → verfügbar: ~9.7 TB ✓
```

### Befehle

```bash
cd /root/dbbackup

# Zuerst Disk-Status prüfen
df -h / /mnt/HC_Volume_104620695 /mnt/smb-devdb

# PostgreSQL zuerst (komplexester Case, pgbench-basiert)
./dbbackup-benchmark.sh --size 100 --engines postgres --smb --runs 2

# Nach erfolgreichem PG-Run: MariaDB
./dbbackup-benchmark.sh --size 100 --engines mariadb --smb --runs 2

# MySQL
./dbbackup-benchmark.sh --size 100 --engines mysql --smb --runs 2
```

**Nicht:** alle 3 auf einmal mit 100 GB — würde 300 GB brauchen.
**Immer:** eine Engine nach der anderen (sequenziell ist das ganze Konzept).

**Erwartete Laufzeit Tag 3:** ~8-12 Stunden (je 2-4 Stunden pro Engine)

---

## Tag 4 — 24.02.2026 — Analyse & Report

**Ziel:** Ergebnisse auswerten, Grafana-Dashboard, finaler Report.

```bash
# Alle Ergebnisse zusammenführen
cat /mnt/HC_Volume_104620695/benchmarks/*/results.csv

# JSON für spätere Analyse
jq '.' /mnt/HC_Volume_104620695/benchmarks/*/results.json
```

### Erwartete Metriken im Report

| Engine | Storage | DB-Größe | Backup-Größe | Kompression | Backup-Zeit | Durchsatz |
|--------|---------|----------|--------------|-------------|-------------|-----------|
| PostgreSQL | lokal | 100 GB | ? GB | ?% | ? min | ? MB/s |
| PostgreSQL | SMB | 100 GB | ? GB | ?% | ? min | ? MB/s |
| MariaDB | lokal | 100 GB | ? GB | ?% | ? min | ? MB/s |
| MariaDB | SMB | 100 GB | ? GB | ?% | ? min | ? MB/s |
| MySQL | lokal | 100 GB | ? GB | ?% | ? min | ? MB/s |
| MySQL | SMB | 100 GB | ? GB | ?% | ? min | ? MB/s |

---

## Notfall-Checkliste (damit es nicht nochmal passiert)

```bash
# Vor jedem Benchmark-Start:
df -h && [ $(df -BG / | awk 'NR==2{gsub(/G/,"",$4); print $4}') -gt 50 ] && echo "OK"

# Nach einem Abbruch aufräumen:
docker compose -f docker-compose.benchmark.yml down -v
rm -rf /mnt/HC_Volume_104620695/benchmarks/UNVOLLSTAENDIG/

# WAL-Archive prüfen (sollte immer 0 sein):
du -sh /mnt/HC_Volume_104620695/qa_wal_archive/
```

---

## Warum es vorher scheiterte

1. **WAL-Archivierung war aktiv** → PostgreSQL schreibt jeden WAL-Chunk (~16 MB) nach
   `/mnt/HC_Volume_104620695/qa_wal_archive/` → bei 100 GB DB entstehen dutzende GB WAL
2. **Kein Disk-Space-Guard** → Script lief weiter bis Disk voll
3. **Backup wurde nicht gelöscht** → Disk bleibt voll für nächste Engine

**Das neue Script (`dbbackup-benchmark.sh`) macht:**
- WAL-Archivierung in postgres-Container deaktivieren (`archive_mode=off`)
- Disk-Prüfung **vor** jedem Schritt
- Backup **sofort löschen** nach der Messung
- Engine-Isolation: nur eine DB-Engine gleichzeitig active
