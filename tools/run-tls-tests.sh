#!/bin/sh
# TLS tests need generated certificates, but no external Redis server.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
zap="${ZAP:-zap}"
case "${1:-}" in
    ''|--check) ;;
    *) echo 'usage: tools/run-tls-tests.sh [--check]' >&2; exit 2 ;;
esac
build=$(mktemp -d "${TMPDIR:-/tmp}/redis-tls-tests.XXXXXX")
trap 'rm -rf "$build"' EXIT HUP INT TERM
cp "$here/src/redis.zap" "$build/redis.zap"
if [ "${1:-}" != --check ]; then
    # Bound regression hangs independently of the operation deadline under test.
    if command -v timeout > /dev/null 2>&1; then
        timeout_command=timeout
    elif command -v gtimeout > /dev/null 2>&1; then
        timeout_command=gtimeout
    else
        echo 'TLS runtime tests require GNU timeout (coreutils on macOS)' >&2
        exit 2
    fi
    sh "$here/tools/generate-tls-fixture.sh" "$build/certs"
    REDIS_TLS_TEST_CERTS="$build/certs"
    SSL_CERT_FILE="$build/certs/ca.pem"
    SSL_CERT_DIR="$build/certs/empty-trust"
    export REDIS_TLS_TEST_CERTS SSL_CERT_FILE SSL_CERT_DIR
fi
failed=0
for source in "$here"/tests/tls/*_test.zap; do
    name=$(basename "$source" .zap)
    cp "$source" "$build/$name.zap"
    if [ "${1:-}" = --check ]; then
        "$zap" ir "$build/$name.zap" > /dev/null
        echo "$name: frontend PASS"
    else
        if "$timeout_command" -k 5s 60s "$zap" run "$build/$name.zap" > "$build/actual"; then
            if ! diff -u "$here/tests/tls/$name.expected" "$build/actual"; then
                failed=1
            fi
        else
            status=$?
            echo "$name: runtime failed (exit $status)" >&2
            failed=1
        fi
        cat "$build/actual"
    fi
done
exit "$failed"
