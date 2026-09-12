#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_root/web"
npm ci
npm run build
cd "$project_root"
cargo build --locked --release
printf 'Built: %s/target/release/nva2dlna\n' "$project_root"

