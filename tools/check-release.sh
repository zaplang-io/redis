#!/bin/sh
# Local/CI preflight. An optional existing tag must match both manifest and HEAD.
# This script never creates a tag or publishes to an external registry.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
version=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$here/zap.toml")
if ! echo "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
    echo 'Expected one stable semver package version in zap.toml' >&2
    exit 1
fi
test -s "$here/LICENSE"
test -s "$here/src/redis.zap"
test -s "$here/CHANGELOG.md"
grep -Fq "## $version " "$here/CHANGELOG.md"
for file in examples/offline.zap examples/basic.zap examples/pipeline_transaction.zap examples/pool.zap examples/blocking.zap examples/pubsub_streams.zap docs/SUPPORT.md docs/RELEASING.md; do
    test -s "$here/$file"
done
if [ "$#" -gt 1 ]; then echo 'usage: tools/check-release.sh [vX.Y.Z]' >&2; exit 2; fi
if [ "$#" = 1 ]; then
    tag=$1
    if [ "$tag" != "v$version" ]; then
        echo "Tag must match package version: v$version" >&2
        exit 1
    fi
    tagged=$(git -C "$here" rev-parse --verify "refs/tags/$tag^{commit}")
    head=$(git -C "$here" rev-parse HEAD)
    if [ "$tagged" != "$head" ]; then
        echo 'Release tag does not identify the checked-out commit' >&2
        exit 1
    fi
fi
echo "Release preflight $version: PASS"
