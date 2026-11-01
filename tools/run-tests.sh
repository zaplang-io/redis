#!/bin/sh
# Stage real modules next to entry files, as required by Zap module resolution.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
zap="${ZAP:-zap}"
build=$(mktemp -d "${TMPDIR:-/tmp}/redis-tests.XXXXXX")
trap 'rm -rf "$build"' EXIT HUP INT TERM
cp "$here/src/redis.zap" "$build/redis.zap"
for source in "$here"/tests/*_test.zap; do
    name=$(basename "$source" .zap)
    cp "$source" "$build/$name.zap"
    if [ "${1:-}" = "--check" ]; then
        "$zap" ir "$build/$name.zap" > /dev/null
        echo "$name: frontend PASS"
    else
        if "$zap" run "$build/$name.zap" > "$build/actual"; then
            :
        else
            cat "$build/actual"
            exit 1
        fi
        diff -u "$here/tests/$name.expected" "$build/actual"
        cat "$build/actual"
    fi
done
