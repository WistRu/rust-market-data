#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <exchange_slug> <public_ws_endpoint>" >&2
    exit 1
fi

exchange_slug="$1"
public_ws_endpoint="$2"

if [[ ! "$exchange_slug" =~ ^[a-z0-9_-]+$ ]]; then
    echo "exchange_slug must contain only lowercase letters, digits, '-' or '_'" >&2
    exit 1
fi

workspace_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
crate_dir="$workspace_root/crates/$exchange_slug"
src_dir="$crate_dir/src"
root_cargo_toml="$workspace_root/Cargo.toml"

if [[ -e "$crate_dir" ]]; then
    echo "crate already exists: $crate_dir" >&2
    exit 1
fi

to_pascal_case() {
    awk -F'[-_]' '{
        for (i = 1; i <= NF; i++) {
            printf toupper(substr($i, 1, 1)) substr($i, 2)
        }
        printf "\n"
    }' <<<"$1"
}

struct_name="$(to_pascal_case "$exchange_slug")Connector"

mkdir -p "$src_dir"

cat > "$crate_dir/Cargo.toml" <<EOF
[package]
name = "$exchange_slug"
version.workspace = true
edition.workspace = true

[dependencies]
common = { path = "../common" }
EOF

cat > "$src_dir/lib.rs" <<EOF
use common::{MarketDataConnector, Subscription};

pub struct $struct_name;

impl MarketDataConnector for $struct_name {
    fn exchange(&self) -> &'static str {
        "$exchange_slug"
    }

    fn ws_endpoint(&self) -> &'static str {
        "$public_ws_endpoint"
    }

    fn build_subscriptions(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .map(|item| format!("{}::{:?}", item.symbol, item.channel))
            .collect()
    }
}
EOF

if ! rg -F "\"crates/$exchange_slug\"" "$root_cargo_toml" >/dev/null 2>&1; then
    perl -0pi -e 's/\[workspace\]\nmembers = \[\n/$&    "crates\/'"$exchange_slug"'",\n/' "$root_cargo_toml"
fi

echo "created crate: $crate_dir"
