# Releasing redis-rs-py

The full release flow, from "I want to ship X.Y.Z" to "the wheel is on PyPI".

**Pre-requisite:** the one-time setup in `docs/PYPI_TRUSTED_PUBLISHER.md` is done.

## 1. Pre-flight on `main`

```bash
# Fetch + reset to a known-good state.
git fetch origin
git checkout main
git pull --ff-only origin main

# Smoke the local build.
uv sync --group dev
uv run maturin develop --release --manifest-path crates/redis-rs-py-driver/Cargo.toml
uv run pytest -n auto

# All green? Move on.
```

## 2. Bump the version

The project version lives in two places that **must** agree:

- `pyproject.toml` `[project] version = "X.Y.Z"`
- `crates/redis-rs-py-driver/Cargo.toml` `[package] version = "X.Y.Z"`

Edit both. Then:

```bash
# Update CHANGELOG.md: rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`
# and add a new empty `## [Unreleased]` section above it.

git add pyproject.toml crates/redis-rs-py-driver/Cargo.toml CHANGELOG.md
git commit -m "chore(release): vX.Y.Z"
git push origin main
```

## 3. Watch `tag.yml` create the tag

`tag.yml` (already shipped) watches for a `chore(release):` commit on `main` and creates a `vX.Y.Z` git tag automatically.

- Confirm the tag is in <https://github.com/oliverhaas/redis-rs-py/tags>.
- Confirm the tag points at the commit you just pushed.

If `tag.yml` failed (e.g. version mismatch between `pyproject.toml` and `Cargo.toml`), fix it and push another `chore(release):` commit.

## 4. Watch `publish.yml`

The tag push triggers `.github/workflows/publish.yml`. The flow:

1. **build** — wheel matrix (6 platforms × 2 Pythons) + sdist.
2. **smoke** — install each wheel in a fresh venv on the matching Python and `import redis_rs_py._driver`.
3. **smoke-sdist** — build the wheel from sdist on Linux, install it, import it.
4. **publish** — gated on the GitHub `pypi` environment (required-reviewer approval). On approve, uploads every artifact to PyPI via OIDC.

Workflow URL: <https://github.com/oliverhaas/redis-rs-py/actions/workflows/publish.yml>

## 5. Verify on PyPI

```bash
# In a clean venv:
uv venv --python 3.14 verify
source verify/bin/activate
uv pip install redis-rs-py==X.Y.Z
python -c "import redis_rs_py; print(redis_rs_py.__version__)"
# Should print X.Y.Z.
```

Also visually inspect <https://pypi.org/project/redis-rs-py/X.Y.Z/> — confirm:

- All 12 wheels are listed (6 platforms × 2 Pythons).
- The sdist is present.
- The README rendered correctly (PyPI uses the README as the long description).

## 6. Post-release

- Create a GitHub Release for the tag (auto-populated from the CHANGELOG): <https://github.com/oliverhaas/redis-rs-py/releases/new>.
- Bump `[Unreleased]` in `CHANGELOG.md` if you haven't already.

## Dry-running against TestPyPI

Before any v0.1.x release, **always** dry-run against TestPyPI first:

```
Actions → Publish to PyPI → Run workflow
  - branch: main
  - version: 0.1.0a2 (the next prerelease)
  - target: testpypi
```

The flow is identical except the upload goes to <https://test.pypi.org/project/redis-rs-py/>. Verify with `pip install --index-url https://test.pypi.org/simple/ redis-rs-py==0.1.0a2`.

## Hotfixes

For a single-commit hotfix on a released version:

```bash
git checkout vX.Y.Z
git checkout -b hotfix/X.Y.Z+1
# fix
git commit -m "fix(...): description"
# bump versions to X.Y.Z+1 in both files
git commit -m "chore(release): vX.Y.(Z+1)"
git push -u origin hotfix/X.Y.Z+1
# open PR → review → merge → tag.yml + publish.yml fire
```

## What to do if a wheel is broken on PyPI

PyPI doesn't allow re-uploading the same filename. If a wheel needs fixing:

1. Yank the broken release: <https://pypi.org/manage/project/redis-rs-py/release/X.Y.Z/> → **Yank release**.
2. Bump to X.Y.Z+1 with the fix and re-release per the normal flow above.

Yanking hides the version from `pip install redis-rs-py` but keeps it accessible to anything that pinned `==X.Y.Z`. That's what you want for a broken wheel.

## cp314 floor note

v0.1 ships cp314 + cp314t wheels only. The wheel matrix is intentionally *not* expanded to cp310–cp313 for v0.1. See `docs/superpowers/plans/19-distribution.md` Task 1 for the full rationale; the short version: the Rust 2024 edition syntax, PyO3 0.28 free-threading APIs, and Python type stubs all assume 3.14+. Revisit for v0.2.
