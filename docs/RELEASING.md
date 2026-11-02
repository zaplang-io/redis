# Preparing and validating a release

The manifest is version 0.1.0; no public v0.1.0 tag is assumed to exist. These
checks prepare a reproducible release. They do not create tags, GitHub releases,
or hosted-registry publications.

## Install the candidate into an application

Use the pinned Zap toolchain from [SUPPORT.md](SUPPORT.md), with LLVM codegen.
Clone the reviewed library checkout and add it as a real path dependency:

```sh
git clone https://github.com/zaplang-io/redis.git redis
# Record/review the full commit before use; switch to v0.1.0 only after it exists.
git -C redis rev-parse HEAD
zap new redis-demo
cd redis-demo
zap add redis --path ../redis
zap zenv create --no-venv
. zenv/activate
cp ../redis/examples/offline.zap src/main.zap
zap run
```

This copies only an application example, not the Redis module. Zap resolves
`import redis` from `zenv/pkg/redis/src/redis.zap`. Keep `zap.toml` and `zap.lock`
in the application. Do not copy redis.zap beside main or activate an unrelated
zenv. The path dependency snapshots the package into the environment; rerun
`zap zenv create --no-venv` after intentionally changing the source checkout.

The pinned CLI records Git dependencies but skips Git materialization during
`zenv create`. Therefore `zap add redis --git ...` is not the verified install
path for this release. Cloning a reviewed commit/tag plus a path dependency
works; the Git tag is a source identity, not a claim of automatic Git fetching.

## Static registry installation

The current Zap publisher supports a local/file:// static index. For a private
local index, use an absolute directory outside the source checkout:

```sh
cd redis
zap publish --dry-run
zap publish --registry file:///absolute/path/to/private-index
```

An external application's manifest can then declare:

```toml
[package]
name = "redis_demo"
version = "0.0.0"

[registries.local]
index = "file:///absolute/path/to/private-index"

[dependencies]
redis = { registry = "redis", version = "0.1.0", index = "local" }
```

Run `zap zenv create --no-venv`, activate it, and import redis. Zap verifies the
archive/tree checksums and records the resolution in `zap.lock`. Version
requirements use the toolchain's minimum-version selection; retain/review the
lockfile for reproducibility. CI exercises this flow using a disposable registry
and a fresh cache, and runs examples from installed packages. It does not publish
to a shared registry. No hosted package registry is advertised here.

## Candidate checks

From the library checkout:

```sh
sh tools/check-release.sh
sh tools/run-package-tests.sh
sh tools/run-tests.sh
sh tools/run-tls-tests.sh
REDIS_TEST_URL=redis://127.0.0.1:6379/0 sh tools/run-integration-tests.sh --isolated
REDIS_EXAMPLE_URL=redis://127.0.0.1:6379/0 sh tools/run-package-tests.sh --live
```

Use only disposable admin-enabled Redis instances for isolated tests. Examples
require an explicit URL, use random key namespaces and fallback TTLs, and delete
their own keys. They never print credentials or flush the database. The offline
package check executes the codec example and compiles every live example;
`--live` runs every example using both path and registry installations.
`--check` performs the same package installations and frontend checks without
native execution. Auth/TLS URL options are accepted by every live example;
verified TLS itself runs with generated certificates in its focused CI suite.

## Version, tag, and release gates

1. Confirm first-release prerequisites #8, #14, #17, #18, #19, and #23 are closed,
   with required CI green and the specific release PR reviewed/merged. Optional
   later roadmap work does not block v0.1 unless deliberately promoted.
2. Update the manifest, changelog and support matrix together; keep the release
   version a stable X.Y.Z and require tag vX.Y.Z to match it. Document changes
   and breaking behavior; do not move an existing release tag.
3. Run all candidate checks above on the exact reviewed commit. The normal CI
   workflow runs on PRs and pushes to main only; feature pushes do not duplicate it.
4. After explicit maintainer approval, create the matching tag on that reviewed
   main commit. Run `sh tools/check-release.sh v0.1.0` from that checkout: the tag
   must resolve to HEAD and match the manifest. The manual **Release validation**
   workflow checks the existing tag, main ancestry, package install and tests.
5. After those checks pass, publish release notes/source or a chosen static index
   through the maintainer's approved release process. Hosted HTTPS writes are a
   toolchain/registry infrastructure concern, not an implemented client feature.

The read-only workflow is a validator, not an automatic publisher. An existing
tag cannot be fully checked before it exists; the PR CI still validates candidate
metadata and the actual path/registry consumer packages before merge.
