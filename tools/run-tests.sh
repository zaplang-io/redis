#!/bin/sh
# Stage real modules next to the entry file, as required by Zap module resolution.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
zap="${ZAP:-zap}"
build=$(mktemp -d "${TMPDIR:-/tmp}/redis-tests.XXXXXX")
trap 'rm -rf "$build"' EXIT HUP INT TERM
cp "$here/src/redis.zap" "$build/redis.zap"
cp "$here/tests/codec_test.zap" "$build/codec_test.zap"
if [ "${1:-}" = "--check" ]; then
    "$zap" ir "$build/codec_test.zap" > /dev/null
    echo "RESP2 codec: frontend PASS"
else
    "$zap" run "$build/codec_test.zap" > "$build/actual"
    printf '%s\n' 'RESP2 codec: PASS' > "$build/expected"
    diff -u "$build/expected" "$build/actual"
    cat "$build/actual"
fi
