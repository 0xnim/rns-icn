# scripts/dev — archived development scripts

One-off debugging, connectivity-check, and manual-exploration scripts written
during development of the ICN-over-RNS prototype. They are **not** part of the
`rns_icn` package, the test suite, or the deployment path — they are kept for
reference only and may be stale.

They were relocated here from the repo root to keep the top level clean. The
root-level `pytest` config (`testpaths = ["tests"]`) intentionally does not
collect the `test_*.py` files in this directory.

For real usage, prefer the packaged entry points: `icn-client`, `icn-server`,
`icn-publish`, `icn-fetch` (see `pyproject.toml`).
