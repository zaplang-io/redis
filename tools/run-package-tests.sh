#!/bin/sh
# Install into external projects via both path dependencies and a temporary
# static registry. Never materialize by manually copying redis.zap beside main.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
zap=${ZAP:-zap}
case "$zap" in
    /*) ;;
    */*) zap=$(cd "$(dirname "$zap")" && pwd)/$(basename "$zap") ;;
    *) zap=$(command -v "$zap") ;;
esac
case "${1:-}" in ''|--check|--live) ;; *) echo 'usage: tools/run-package-tests.sh [--check|--live]' >&2; exit 2 ;; esac
build=$(mktemp -d "${TMPDIR:-/tmp}/redis-package.XXXXXX")
trap 'rm -rf "$build"' EXIT HUP INT TERM
ZAP_CACHE="$build/cache"
unset ZAP_ZENV
export ZAP_CACHE
version=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$here/zap.toml")
run_example() {
    source=$1
    if [ "$mode" = --check ]; then
        "$zap" ir "src/$source.zap" > /dev/null
    else
        "$timeout_command" -k 5s 60s "$zap" run "src/$source.zap" > actual
        diff -u "$here/examples/$source.expected" actual
        cat actual
    fi
}
mode=${1:-}
if [ "$mode" = --live ] && [ -z "${REDIS_EXAMPLE_URL:-}" ]; then
    echo 'Set REDIS_EXAMPLE_URL explicitly for live package examples' >&2
    exit 2
fi
if [ "$mode" != --check ]; then
    if command -v timeout > /dev/null 2>&1; then timeout_command=timeout
    elif command -v gtimeout > /dev/null 2>&1; then timeout_command=gtimeout
    else echo 'Runtime package tests require GNU timeout' >&2; exit 2
    fi
fi
# Validate packaging without writing to a registry, then exercise a disposable
# local registry to verify archive/checksum/import behavior end to end.
(cd "$here" && "$zap" publish --dry-run)
(cd "$here" && "$zap" publish --registry "file://$build/index")
for kind in path registry; do
    consumer="$build/$kind-consumer"
    mkdir -p "$consumer/src"
    cp "$here"/examples/*.zap "$consumer/src/"
    cat > "$consumer/zap.toml" <<MANIFEST
[package]
name = "redis_consumer"
version = "0.0.0"
MANIFEST
    (
        cd "$consumer"
        if [ "$kind" = path ]; then
            "$zap" add redis --path "$here"
        else
            cat >> zap.toml <<MANIFEST
[registries.fixture]
index = "file://$build/index"
[dependencies]
redis = { registry = "redis", version = "$version", index = "fixture" }
MANIFEST
        fi
        "$zap" zenv create --no-venv
        ZAP_ZENV="$consumer/zenv"
        export ZAP_ZENV
        test -f zap.lock
        test -f "$ZAP_ZENV/pkg/redis/src/redis.zap"
        for source in offline basic pipeline_transaction pool blocking; do
            if [ "$mode" = --check ] || [ "$source" = offline ] || [ "$mode" = --live ]; then
                run_example "$source"
            else
                "$zap" ir "src/$source.zap" > /dev/null
            fi
        done
        echo "$kind consumer: PASS"
    )
done
