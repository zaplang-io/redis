#!/bin/sh
# Explicit opt-in: never silently connect to a developer's localhost Redis.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
zap="${ZAP:-zap}"
check=0
isolated=0
for arg do
    case "$arg" in
        --check) check=1 ;;
        --isolated) isolated=1 ;;
        *) echo 'usage: tools/run-integration-tests.sh [--check] [--isolated]' >&2; exit 2 ;;
    esac
done
if [ "$check" = 0 ] && [ -z "${REDIS_TEST_URL:-}" ]; then
    echo 'Set REDIS_TEST_URL explicitly to run live integration tests.' >&2
    exit 2
fi
if [ "$isolated" = 1 ]; then
    REDIS_TEST_ISOLATED=1
    export REDIS_TEST_ISOLATED
fi
build=$(mktemp -d "${TMPDIR:-/tmp}/redis-live-tests.XXXXXX")
trap 'rm -rf "$build"' EXIT HUP INT TERM
cp "$here/src/redis.zap" "$build/redis.zap"
cp "$here/tests/integration/integration_fixtures.zap" "$build/"
failed=0
for source in "$here"/tests/integration/*_test.zap; do
    name=$(basename "$source" .zap)
    if [ "$name" = session_test ] && [ "$isolated" = 0 ] && [ "$check" = 0 ]; then
        echo 'session_test: not selected (use --isolated on a disposable admin-enabled instance)'
        continue
    fi
    cp "$source" "$build/$name.zap"
    if [ "$check" = 1 ]; then
        "$zap" ir "$build/$name.zap" > /dev/null
        echo "$name: frontend PASS"
    else
        if "$zap" run "$build/$name.zap" > "$build/actual"; then
            if ! diff -u "$here/tests/integration/$name.expected" "$build/actual"; then
                failed=1
            fi
        else
            failed=1
            if [ "$name" = transaction_pool_test ]; then
                # Failure-only diagnostics; never replace the original failure
                # status with a successful alternate collector/scheduler mode.
                if "$zap" build "$build/$name.zap" -o "$build/pool-lifetime-debug"; then
                    echo 'pool lifetime diagnostic: no evacuation' >&2
                    ZAP_GC_NO_EVAC=1 "$build/pool-lifetime-debug" >&2 || :
                    echo 'pool lifetime diagnostic: one worker' >&2
                    MAXPROCS=1 "$build/pool-lifetime-debug" >&2 || :
                fi
            fi
        fi
        cat "$build/actual"
    fi
done
exit "$failed"
