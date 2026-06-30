#!/usr/bin/env bash
set -euo pipefail

runtime_root="${MEXC_OBSERVER_RUNTIME_ROOT:-/opt/rust-market-data/runtime/mexc-observers}"
run_dir="${1:-}"

if [[ -z "$run_dir" ]]; then
    run_dir="$(find "$runtime_root" -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)"
fi

if [[ -z "$run_dir" || ! -d "$run_dir" ]]; then
    echo "observer run directory not found" >&2
    exit 1
fi

signal_pid_path="$run_dir/futures-contract-signal.pid"
soak_pid_path="$run_dir/public-soak-report.pid"
signal_summary_path="$run_dir/futures-contract-signal-summary.json"
soak_report_path="$run_dir/public-soak-report.json"
metadata_path="$run_dir/launcher-metadata.txt"

signal_pid="$(cat "$signal_pid_path" 2>/dev/null || true)"
soak_pid="$(cat "$soak_pid_path" 2>/dev/null || true)"

echo "run_dir=$run_dir"
if [[ -f "$metadata_path" ]]; then
    grep -E '^(started_at_utc|signal_watch_seconds|soak_watch_seconds|checkpoint_seconds)=' "$metadata_path" || true
fi
echo "signal_pid=${signal_pid:-none}"
echo "soak_pid=${soak_pid:-none}"

if [[ -n "$signal_pid" || -n "$soak_pid" ]]; then
    ps -p "${signal_pid:-0},${soak_pid:-0}" -o pid=,stat=,etime=,cmd= 2>/dev/null || true
fi

if [[ -f "$signal_summary_path" ]]; then
    echo "signal_summary:"
    jq -r '[
      "  kind=\(.kind)",
      "  elapsed_ms=\(.elapsed_ms)",
      "  watch_forever=\(.watch_forever)",
      "  typed_contract_payloads=\(.typed_contract_payloads)",
      "  typed_event_contract_payloads=\(.typed_event_contract_payloads)",
      "  signal_refreshes=\(.signal_refreshes)",
      "  last_signal_kind=\(.last_signal_kind // "none")",
      "  last_signal_symbol=\(.last_signal_symbol // "none")",
      "  stream_errors=\(.stream_errors)"
    ][]' "$signal_summary_path"
fi

if [[ -f "$soak_report_path" ]]; then
    echo "soak_report:"
    jq -r '[
      "  kind=\(.kind)",
      "  complete=\(.complete)",
      "  elapsed_seconds=\(.elapsed_seconds)",
      "  total_live_events=\(.total_live_events)",
      "  healthy=\(.health.healthy)",
      "  checkpoint_count=\(.checkpoints | length)",
      "  futures_total=\(.state_handoff.futures.total_symbols)",
      "  with_contract=\(.state_handoff.futures.with_contract)",
      "  without_contract=\(.state_handoff.futures.without_contract)",
      "  with_event_contract=\(.state_handoff.futures.with_event_contract)",
      "  with_ticker=\(.state_handoff.futures.with_ticker)",
      "  with_depth=\(.state_handoff.futures.with_depth)"
    ][]' "$soak_report_path"
    jq -r '"  without_contract_symbols=" + ((.state_handoff.futures.without_contract_symbols // []) | join(","))' "$soak_report_path"
    jq -r '"  with_event_contract_symbols=" + ((.state_handoff.futures.with_event_contract_symbols // []) | join(","))' "$soak_report_path"
fi
