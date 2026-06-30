#!/usr/bin/env bash
set -euo pipefail

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

printf "%-16s | %s\n" "exchange" "public_ws_endpoint"
printf -- "-----------------+------------------------------------------------------\n"

for cargo_toml in "$workspace_root"/crates/*/Cargo.toml; do
    crate_dir="$(dirname "$cargo_toml")"
    exchange_slug="$(basename "$crate_dir")"

    if [[ "$exchange_slug" == "common" ]]; then
        continue
    fi

    lib_rs="$crate_dir/src/lib.rs"
    endpoint="n/a"

    if [[ -f "$lib_rs" ]]; then
        endpoint="$(
            awk '
                /fn ws_endpoint\(&self\)/ { in_fn = 1; next }
                in_fn && match($0, /"[^"]+"/) {
                    print substr($0, RSTART + 1, RLENGTH - 2)
                    exit
                }
                in_fn && /\}/ { exit }
            ' "$lib_rs"
        )"
        endpoint="${endpoint:-n/a}"
    fi

    printf "%-16s | %s\n" "$exchange_slug" "$endpoint"
done | sort
