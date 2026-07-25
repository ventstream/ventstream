# Contributing to VentStream

VentStream accepts focused bug fixes, connector improvements, tests, and
documentation changes.

## Before opening a pull request

1. Open an issue for significant behavioral or architectural changes.
2. Keep changes scoped to one problem.
3. Add tests for changed behavior and failure paths.
4. Do not include customer data, credentials, private endpoints, or
   organization-specific configuration.
5. Run the local checks:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Connector changes should also exercise snapshot-to-stream transitions,
ordering, restart recovery, deletes, backpressure, and durable checkpoint
behavior where applicable.

## Contribution terms

By submitting a contribution, you certify that you have the right to submit it
under the Apache License 2.0 and agree that it may be distributed under that
license. Commits must include a Developer Certificate of Origin sign-off:

```text
Signed-off-by: Your Name <your-email@example.com>
```

Use `git commit -s` to add the sign-off.
