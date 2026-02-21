#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# bench-tmux.sh  —  Benchmark in tmux starten mit Live-Monitoring
#
# Layout:
#   ┌───────────────────────────┬───────────────────────────┐
#   │                           │  Disk-Watch               │
#   │  BENCHMARK (läuft durch)  ├───────────────────────────┤
#   │                           │  Docker-Container         │
#   └───────────────────────────┴───────────────────────────┘
#
# Usage:
#   ./bench-tmux.sh [--size GB] [--engines LIST] [--smb] [--runs N]
#   ./bench-tmux.sh --attach       # Nur zu laufender Session verbinden
#   ./bench-tmux.sh --kill         # Session beenden
#
# Optionen werden direkt an dbbackup-benchmark.sh weitergegeben.
# ─────────────────────────────────────────────────────────────────────────────

SESSION="dbbackup-bench"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="/mnt/HC_Volume_104620695/benchmarks/bench_$(date +%Y%m%d_%H%M).log"

# ── Sondermodi ────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--attach" ]]; then
    tmux attach -t "$SESSION" 2>/dev/null || { echo "Keine Session '$SESSION' aktiv."; exit 1; }
    exit 0
fi

if [[ "${1:-}" == "--kill" ]]; then
    tmux kill-session -t "$SESSION" 2>/dev/null && echo "Session '$SESSION' beendet." || echo "Keine Session aktiv."
    exit 0
fi

# ── Bestehende Session schließen ──────────────────────────────────────────────
tmux kill-session -t "$SESSION" 2>/dev/null || true

# ── Argumente für benchmark-script ───────────────────────────────────────────
BENCH_ARGS="$*"
[[ -z "$BENCH_ARGS" ]] && BENCH_ARGS="--size 10 --engines postgres,mariadb,mysql"

mkdir -p /mnt/HC_Volume_104620695/benchmarks

# ── Session + Layout aufbauen ─────────────────────────────────────────────────

# Neue Session, linkes Haupt-Pane (Benchmark)
tmux new-session -d -s "$SESSION" -x 220 -y 50

# Rechte Spalte abtrennen (40% Breite)
tmux split-window -h -p 40 -t "$SESSION:0"

# Rechte Spalte in zwei Zeilen teilen (oben: Disk, unten: Docker)
tmux split-window -v -p 50 -t "$SESSION:0.1"

# ── Pane-Inhalte befüllen ─────────────────────────────────────────────────────

# Pane 0 (links) — Benchmark
tmux send-keys -t "$SESSION:0.0" \
    "cd ${SCRIPT_DIR} && echo 'Log: ${LOG}' && ./dbbackup-benchmark.sh ${BENCH_ARGS} 2>&1 | tee '${LOG}'" Enter

# Pane 1 (rechts oben) — Disk-Watch
tmux send-keys -t "$SESSION:0.1" \
    "watch -n 5 'echo \"=== Disk ===\"; df -h / /mnt/HC_Volume_104620695 /mnt/smb-devdb 2>/dev/null; echo; echo \"=== Docker Volumes ===\"; docker system df 2>/dev/null | head -10'" Enter

# Pane 2 (rechts unten) — Docker-Container
tmux send-keys -t "$SESSION:0.2" \
    "watch -n 3 'echo \"=== Container ===\"; docker ps --format \"table {{.Names}}\\t{{.Status}}\\t{{.Ports}}\" 2>/dev/null; echo; echo \"=== CPU/RAM ===\"; top -bn1 | head -8'" Enter

# ── Fokus auf Benchmark-Pane ──────────────────────────────────────────────────
tmux select-pane -t "$SESSION:0.0"

# ── Info ausgeben ─────────────────────────────────────────────────────────────
echo ""
echo "  tmux-Session '$SESSION' gestartet"
echo ""
echo "  Argumente:   ${BENCH_ARGS}"
echo "  Log-Datei:   ${LOG}"
echo ""
echo "  Verbinden:   tmux attach -t ${SESSION}"
echo "  Beenden:     ./bench-tmux.sh --kill  oder  Ctrl+b d (detach)"
echo ""
echo "  Tasten:"
echo "    Ctrl+b d       Detach (Session läuft weiter)"
echo "    Ctrl+b →/←     Zwischen Panes wechseln"
echo "    Ctrl+b z       Aktuellen Pane maximieren/zurück"
echo "    Ctrl+b [       Scroll-Modus  (q zum Beenden)"
echo ""

# Direkt verbinden
tmux attach -t "$SESSION"
