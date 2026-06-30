#!/usr/bin/env bash
set -euo pipefail

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="$workspace_root/target/debug/examples"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output_root_default="$workspace_root/runtime/mexc-observers/$timestamp"

watch_seconds="${MEXC_OBSERVER_SECONDS:-1800}"
signal_watch_seconds="${MEXC_CONTRACT_SIGNAL_OBSERVER_SECONDS:-$watch_seconds}"
soak_watch_seconds="${MEXC_SOAK_OBSERVER_SECONDS:-$watch_seconds}"
checkpoint_seconds="${MEXC_OBSERVER_CHECKPOINT_SECONDS:-60}"
signal_refresh="${MEXC_CONTRACT_SIGNAL_REFRESH:-1}"
output_root="${MEXC_OBSERVER_OUTPUT_ROOT:-$output_root_default}"

mkdir -p "$output_root"

if ! command -v setsid >/dev/null 2>&1; then
    echo "setsid is required but not found" >&2
    exit 1
fi

echo "building mexc observer examples..."
cargo build -q -p mexc --example futures_contract_signal_feed --example public_soak_report \
    --manifest-path "$workspace_root/Cargo.toml"

signal_bin="$target_dir/futures_contract_signal_feed"
soak_bin="$target_dir/public_soak_report"

if [[ ! -x "$signal_bin" ]]; then
    echo "missing example binary: $signal_bin" >&2
    exit 1
fi

if [[ ! -x "$soak_bin" ]]; then
    echo "missing example binary: $soak_bin" >&2
    exit 1
fi

signal_feed_path="$output_root/futures-contract-signal.ndjson"
signal_summary_path="$output_root/futures-contract-signal-summary.json"
signal_log_path="$output_root/futures-contract-signal.log"
signal_pid_path="$output_root/futures-contract-signal.pid"

soak_report_path="$output_root/public-soak-report.json"
soak_log_path="$output_root/public-soak-report.log"
soak_pid_path="$output_root/public-soak-report.pid"

metadata_path="$output_root/launcher-metadata.txt"

(
    cd "$workspace_root"
    env \
        MEXC_CONTRACT_SIGNAL_FEED_SECONDS="$signal_watch_seconds" \
        MEXC_CONTRACT_SIGNAL_CHECKPOINT_SECONDS="$checkpoint_seconds" \
        MEXC_CONTRACT_SIGNAL_REFRESH="$signal_refresh" \
        MEXC_CONTRACT_SIGNAL_FEED_PATH="$signal_feed_path" \
        MEXC_CONTRACT_SIGNAL_SUMMARY_PATH="$signal_summary_path" \
        setsid "$signal_bin" \
        >"$signal_log_path" 2>&1 < /dev/null &
    echo $! >"$signal_pid_path"
)

(
    cd "$workspace_root"
    env \
        MEXC_SOAK_SECONDS="$soak_watch_seconds" \
        MEXC_SOAK_CHECKPOINT_EVERY_SECONDS="$checkpoint_seconds" \
        MEXC_SOAK_REPORT_PATH="$soak_report_path" \
        setsid "$soak_bin" \
        >"$soak_log_path" 2>&1 < /dev/null &
    echo $! >"$soak_pid_path"
)

{
    echo "started_at_utc=$timestamp"
    echo "workspace_root=$workspace_root"
    echo "default_watch_seconds=$watch_seconds"
    echo "signal_watch_seconds=$signal_watch_seconds"
    echo "soak_watch_seconds=$soak_watch_seconds"
    echo "checkpoint_seconds=$checkpoint_seconds"
    echo "signal_refresh=$signal_refresh"
    echo "signal_feed_path=$signal_feed_path"
    echo "signal_summary_path=$signal_summary_path"
    echo "signal_log_path=$signal_log_path"
    echo "signal_pid=$(cat "$signal_pid_path")"
    echo "soak_report_path=$soak_report_path"
    echo "soak_log_path=$soak_log_path"
    echo "soak_pid=$(cat "$soak_pid_path")"
} >"$metadata_path"

echo "output_root=$output_root"
echo "signal_pid=$(cat "$signal_pid_path")"
echo "soak_pid=$(cat "$soak_pid_path")"
echo "signal_feed_path=$signal_feed_path"
echo "signal_summary_path=$signal_summary_path"
echo "soak_report_path=$soak_report_path"
