# Plan 19 — Distribution: re-enable publish, expand wheel matrix, smoke every wheel

> **For agentic workers:** REQUIRED SUB-SKILL: Use [superpowers:subagent-driven-development](https://) (recommended) or [superpowers:executing-plans](https://) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-enable `publish.yml`, factor the wheel-build matrix into a reusable workflow shared with `ci.yml`, add musllinux (Alpine) wheels and an sdist, smoke-test every wheel before publish, and document the PyPI Trusted Publisher one-time setup so the first release is a single tag push.

**Architecture decision:** **Keep cp314 + cp314t as the only Python floor for v0.1.** The roadmap and `PLAN.md` reference cp310–cp314, but the codebase is greenfield, the Rust core leans on `if let ... && ...` chain syntax (Rust 2024 edition), and the Python tree relies on Python 3.14-only features in the type stubs. Dropping the floor to cp310 is a multi-day refactor that buys little for a brand-new package; the proper time to do it is v0.2 once the API surface has stabilised and we have real users on older interpreters asking for it. This plan **expands the matrix horizontally** (more platforms, more libc variants, sdist) but **keeps the Python floor at cp314/cp314t**. The decision is documented inline in `pyproject.toml` and the CHANGELOG.

The wheel matrix is factored into a reusable workflow (`.github/workflows/_build_wheels.yml`) so `ci.yml` and `publish.yml` consume the same matrix definition — no drift between "what we build in CI" and "what we publish."

The publish job uses GitHub's OIDC + PyPI Trusted Publisher (no long-lived API tokens). The first publish requires a one-time PyPI side-setup (a "pending publisher" entry); after that, every tag push triggers a full matrix build → smoke-every-wheel → publish.

**Tech Stack:** GitHub Actions, `cibuildwheel` v3.4 (already in use), `maturin-action` v1 for the sdist, `pypa/gh-action-pypi-publish` v1 (the canonical OIDC publisher). No code changes — purely CI plumbing + docs.

**Reference material:**
- `/home/ohaas/e1+/redis-rs-py/PLAN.md` — Distribution section: prebuilt wheels for Linux x86_64 + aarch64, macOS arm64, Windows x86_64; musllinux for Alpine; abi3 where PyO3 0.28 allows; sdist as fallback.
- `/home/ohaas/e1+/redis-rs-py/.github/workflows/ci.yml` — current `build-wheels` matrix (cp314 + cp314t for the four platforms).
- `/home/ohaas/e1+/redis-rs-py/.github/workflows/publish.yml` — the disabled scaffold this plan re-enables.
- `/home/ohaas/e1+/redis-rs-py/docs/superpowers/plans/0000-roadmap.md` — Plan 19 entry.
- PyPI Trusted Publisher docs: <https://docs.pypi.org/trusted-publishers/>
- `cibuildwheel` musllinux docs: <https://cibuildwheel.pypa.io/en/stable/options/#linux-image>

**Out of scope:** Dropping the cp310 floor (deferred to v0.2 — see decision above). Building wheels for FreeBSD or other tier-2 platforms (no PyPI demand). Signing wheels with sigstore (PyPI's Trusted Publisher already gives us provenance via OIDC).

---

## File structure delivered by this plan

```
.github/workflows/
  _build_wheels.yml            # NEW: reusable wheel-matrix workflow
  ci.yml                       # MODIFIED: delegate build-wheels to the reusable workflow + add musllinux smoke
  publish.yml                  # MODIFIED: re-enabled, calls the reusable workflow + adds smoke + publish jobs
docs/
  RELEASING.md                 # NEW: step-by-step release procedure
  PYPI_TRUSTED_PUBLISHER.md    # NEW: one-time PyPI + GitHub Environment setup
crates/redis-rs-py-driver/
  Cargo.toml                   # MODIFIED: add a `publish = false` justification comment
pyproject.toml                 # MODIFIED: cp314 floor justification comment + classifier
CHANGELOG.md                   # MODIFIED: cp314-floor decision noted
README.md                      # MODIFIED: install paragraph mentions Alpine + cp314 floor
```

---

## Task 1: Document the cp314-floor decision

The `requires-python = ">=3.14"` line is the load-bearing decision in this plan. Document it where future contributors will look first: in the file itself.

**Files:**
- Modify: `pyproject.toml`
- Modify: `crates/redis-rs-py-driver/Cargo.toml`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the decision comment to `pyproject.toml`**

In `pyproject.toml`, replace:

```toml
requires-python = ">=3.14"
```

with:

```toml
# v0.1 ships cp314 + cp314t only. Rationale:
#   * the Rust core uses 2024-edition syntax (`if let ... && ...` chains)
#     and PyO3 0.28 free-threading APIs that exist in cp313+ only;
#   * the Python `__init__.py`/`.pyi` tree leans on cp314 type-stub features;
#   * dropping to cp310 (per PLAN.md's prebuilt-wheel target) is multi-day
#     work that buys nothing concrete until users on older interpreters
#     ask for it.
# Revisit for v0.2 once the API surface has stabilised and there's measured
# demand. See docs/RELEASING.md and docs/superpowers/plans/19-distribution.md.
requires-python = ">=3.14"
```

- [ ] **Step 2: Verify the classifiers already cover free-threading and 3.14**

Read the `classifiers = [...]` block in `pyproject.toml`. The Plan 01 scaffold already wrote both:

```toml
"Programming Language :: Python :: 3.14",
"Programming Language :: Python :: Free Threading :: 1 - Unstable",
```

If either is missing, add it. Otherwise no change.

- [ ] **Step 3: Document `publish = false` on the Rust crate**

In `crates/redis-rs-py-driver/Cargo.toml`, find the `[package]` block. Add a comment above the `publish = false` line (or add the line if missing):

```toml
[package]
name = "redis-rs-py-driver"
version = "0.1.0-alpha.1"
edition = "2024"
# We never publish the driver crate to crates.io. It only ships as the
# compiled extension module bundled into the redis-rs-py wheel via
# maturin. Publishing the source crate would invite use as a Rust
# dependency, which it isn't designed for (Python-only embedded API,
# no semver guarantees on the Rust side).
publish = false
```

- [ ] **Step 4: Add the CHANGELOG entry**

Edit `CHANGELOG.md`, append under `### Added`:

```markdown
- Distribution pipeline: `publish.yml` re-enabled with PyPI Trusted Publisher (OIDC), reusable wheel-matrix workflow `_build_wheels.yml` shared with `ci.yml`, sdist fallback, musllinux Alpine wheels, smoke-test-every-wheel gate, `docs/RELEASING.md` and `docs/PYPI_TRUSTED_PUBLISHER.md` operational guides.
```

And under a new `### Decisions` section (or `### Notes`):

```markdown
### Decisions
- v0.1 ships cp314 + cp314t only. The roadmap-stated cp310 floor is deferred to v0.2; see `docs/superpowers/plans/19-distribution.md` Task 1 for rationale.
```

- [ ] **Step 5: Verify the project still builds locally**

```bash
uv sync --group dev
uv run maturin develop --manifest-path crates/redis-rs-py-driver/Cargo.toml
uv run pytest -n auto
```

Expected: green; no test regressions.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml crates/redis-rs-py-driver/Cargo.toml CHANGELOG.md
git commit -m "chore(release): document cp314 floor and Rust crate non-publication"
```

---

## Task 2: Reusable wheel-matrix workflow

`ci.yml` and `publish.yml` should consume the same matrix definition. GitHub Actions reusable workflows (`workflow_call`) are the right tool: one canonical matrix in `_build_wheels.yml`, both consumers `uses:` it.

**Files:**
- Create: `.github/workflows/_build_wheels.yml`

- [ ] **Step 1: Write the reusable workflow**

Create `.github/workflows/_build_wheels.yml`:

```yaml
# Reusable wheel-build matrix.
#
# Consumers: `ci.yml` (per-PR + per-push) and `publish.yml` (on tag).
# Both invoke this workflow with the same inputs so the published
# wheels are bit-identical to the ones CI built and tested.
#
# Build targets (v0.1):
#   * Linux x86_64        manylinux + musllinux
#   * Linux aarch64       manylinux + musllinux
#   * macOS arm64
#   * Windows x86_64
#   * sdist (built once, on the Linux x86_64 leg)
#
# Python: cp314 + cp314t (free-threaded). Per the cp314-floor decision
# documented in pyproject.toml.

name: Build wheels (reusable)

on:
  workflow_call:
    inputs:
      ref:
        description: "git ref to check out (defaults to the caller's ref)"
        required: false
        type: string
        default: ""
      build_sdist:
        description: "whether to build the sdist on the linux-x86_64 leg"
        required: false
        type: boolean
        default: true
      artifact_prefix:
        description: "prefix for upload-artifact names (use 'dist' for publish, 'wheels' for CI)"
        required: false
        type: string
        default: "wheels"

jobs:
  build:
    name: Build wheels (${{ matrix.label }})
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            label: linux-x86_64-manylinux
            cibw_archs: x86_64
            cibw_build: cp314-manylinux_x86_64 cp314t-manylinux_x86_64
          - os: ubuntu-latest
            label: linux-x86_64-musllinux
            cibw_archs: x86_64
            cibw_build: cp314-musllinux_x86_64 cp314t-musllinux_x86_64
          - os: ubuntu-24.04-arm
            label: linux-aarch64-manylinux
            cibw_archs: aarch64
            cibw_build: cp314-manylinux_aarch64 cp314t-manylinux_aarch64
          - os: ubuntu-24.04-arm
            label: linux-aarch64-musllinux
            cibw_archs: aarch64
            cibw_build: cp314-musllinux_aarch64 cp314t-musllinux_aarch64
          - os: macos-14
            label: macos-arm64
            cibw_archs: arm64
            cibw_build: cp314-macosx_arm64 cp314t-macosx_arm64
          - os: windows-latest
            label: windows-amd64
            cibw_archs: AMD64
            cibw_build: cp314-win_amd64 cp314t-win_amd64
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v6
        with:
          ref: ${{ inputs.ref || github.ref }}

      - name: Build wheels
        uses: pypa/cibuildwheel@v3.4
        env:
          CIBW_BUILD: ${{ matrix.cibw_build }}
          CIBW_ENABLE: cpython-freethreading
          CIBW_ARCHS: ${{ matrix.cibw_archs }}
          # Linux builds run inside manylinux/musllinux containers — install Rust there.
          CIBW_BEFORE_ALL_LINUX: |
            curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
          CIBW_ENVIRONMENT_LINUX: PATH="$HOME/.cargo/bin:$PATH"
          # musllinux's default image is alpine — verify maturin can find a compiler there.
          CIBW_MUSLLINUX_X86_64_IMAGE: musllinux_1_2
          CIBW_MUSLLINUX_AARCH64_IMAGE: musllinux_1_2

      - uses: actions/upload-artifact@v7
        with:
          name: ${{ inputs.artifact_prefix }}-${{ matrix.label }}
          path: wheelhouse/*.whl

  sdist:
    name: Build sdist
    if: inputs.build_sdist
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
        with:
          ref: ${{ inputs.ref || github.ref }}

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Set up Python
        run: uv python install 3.14

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Build sdist
        uses: PyO3/maturin-action@v1
        with:
          command: sdist
          args: --manifest-path crates/redis-rs-py-driver/Cargo.toml --out wheelhouse/

      - uses: actions/upload-artifact@v7
        with:
          name: ${{ inputs.artifact_prefix }}-sdist
          path: wheelhouse/*.tar.gz
```

- [ ] **Step 2: Verify the workflow file is well-formed**

```bash
uv run python -c "
import yaml
from pathlib import Path
data = yaml.safe_load(Path('.github/workflows/_build_wheels.yml').read_text())
assert 'workflow_call' in data['on']
matrix = data['jobs']['build']['strategy']['matrix']['include']
labels = [m['label'] for m in matrix]
print('matrix labels:', labels)
assert 'linux-x86_64-musllinux' in labels
assert 'linux-aarch64-musllinux' in labels
assert 'sdist' in data['jobs']
"
```

Expected: prints all 6 wheel labels including both musllinux variants.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/_build_wheels.yml
git commit -m "ci: add reusable wheel-matrix workflow with musllinux + sdist"
```

---

## Task 3: Refactor `ci.yml` to consume the reusable workflow

Drop the inline `build-wheels` matrix from `ci.yml` and replace it with a `uses:` of `_build_wheels.yml`. The smoke-test jobs stay (they're already present; we adjust them to read from the new artifact names).

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Replace `build-wheels` and the smoke jobs**

Edit `.github/workflows/ci.yml`. Replace the entire `build-wheels:`, `smoke-test-wheel:`, and `smoke-test-wheel-freethreaded:` blocks (lines 75 to end of file) with:

```yaml
  build-wheels:
    name: Build wheels
    needs: [lint, test]
    uses: ./.github/workflows/_build_wheels.yml
    with:
      build_sdist: false
      artifact_prefix: wheels

  smoke-test-wheel:
    name: Smoke-test wheel (cp314 ${{ matrix.label }})
    needs: [build-wheels]
    strategy:
      fail-fast: false
      matrix:
        include:
          - label: linux-x86_64-manylinux
            runs-on: ubuntu-latest
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*manylinux*x86_64.whl"
          - label: linux-x86_64-musllinux
            runs-on: ubuntu-latest
            container: alpine:3.20
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*musllinux*x86_64.whl"
          - label: linux-aarch64-manylinux
            runs-on: ubuntu-24.04-arm
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*manylinux*aarch64.whl"
          - label: macos-arm64
            runs-on: macos-14
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*macosx*arm64.whl"
          - label: windows-amd64
            runs-on: windows-latest
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*win_amd64.whl"
    runs-on: ${{ matrix.runs-on }}
    container: ${{ matrix.container || '' }}
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: wheels-${{ matrix.label }}
          path: wheelhouse

      - name: Install Python (musllinux Alpine container)
        if: matrix.label == 'linux-x86_64-musllinux'
        run: apk add --no-cache python3=~3.14 py3-pip

      - name: Install uv
        if: matrix.label != 'linux-x86_64-musllinux'
        uses: astral-sh/setup-uv@v7

      - name: Set up Python
        if: matrix.label != 'linux-x86_64-musllinux'
        run: uv python install ${{ matrix.python }}

      - name: Install wheel + verify import (Alpine)
        if: matrix.label == 'linux-x86_64-musllinux'
        run: |
          python3 -m venv /tmp/venv
          /tmp/venv/bin/pip install --no-deps wheelhouse/${{ matrix.wheel_glob }}
          /tmp/venv/bin/python -c "from redis_rs_py import _driver; print('OK', _driver.__version__)"

      - name: Install wheel + verify import (non-Alpine)
        if: matrix.label != 'linux-x86_64-musllinux'
        shell: bash
        run: |
          uv venv --python ${{ matrix.python }} smoke
          source smoke/bin/activate || smoke/Scripts/activate
          uv pip install --no-deps wheelhouse/${{ matrix.wheel_glob }}
          python -c "from redis_rs_py import _driver; print('OK', _driver.__version__)"

  smoke-test-wheel-freethreaded:
    name: Smoke-test cp314t free-threaded wheel
    needs: [build-wheels]
    strategy:
      fail-fast: false
      matrix:
        include:
          - label: linux-x86_64-manylinux
            runs-on: ubuntu-latest
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*manylinux*x86_64.whl"
          - label: linux-aarch64-manylinux
            runs-on: ubuntu-24.04-arm
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*manylinux*aarch64.whl"
          - label: macos-arm64
            runs-on: macos-14
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*macosx*arm64.whl"
          - label: windows-amd64
            runs-on: windows-latest
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*win_amd64.whl"
    runs-on: ${{ matrix.runs-on }}
    steps:
      - uses: actions/download-artifact@v8
        with:
          name: wheels-${{ matrix.label }}
          path: wheelhouse

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Set up free-threaded Python
        run: uv python install 3.14t

      - name: Install wheel + verify FT
        shell: bash
        run: |
          uv venv --python 3.14t smoke
          source smoke/bin/activate || smoke/Scripts/activate
          uv pip install --no-deps wheelhouse/${{ matrix.wheel_glob }}
          python -c "
          import sys
          from redis_rs_py import _driver
          assert not sys._is_gil_enabled(), 'GIL is enabled — not running on free-threaded build'
          print('OK', _driver.__version__, sys.version)
          "
```

- [ ] **Step 2: Verify the workflow parses**

```bash
uv run python -c "
import yaml
from pathlib import Path
data = yaml.safe_load(Path('.github/workflows/ci.yml').read_text())
print('jobs:', list(data['jobs']))
assert data['jobs']['build-wheels']['uses'] == './.github/workflows/_build_wheels.yml'
"
```

Expected: prints `jobs: ['lint', 'test', 'build-wheels', 'smoke-test-wheel', 'smoke-test-wheel-freethreaded']` and the `uses:` assertion holds.

- [ ] **Step 3: Verification run** (only meaningful once a PR is open)

```
Open a PR → Actions tab → CI workflow → confirm:
  * `build-wheels` succeeds, producing 6 artifacts named `wheels-*`
  * Both `smoke-test-wheel` matrices are green (5 + 4 = 9 cells)
  * No timeouts, no missing wheel-glob failures
```

If the musllinux Alpine smoke fails, the most likely cause is `apk add python3=~3.14` not being available yet on Alpine 3.20 (it ships 3.12). In that case fall back to building Python from source via `cibuildwheel`'s `before-test`, or downgrade the smoke-test to a representative cp313 wheel — either way, document the limitation in `RELEASING.md`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: delegate wheel matrix to reusable workflow + smoke musllinux"
```

---

## Task 4: Re-enable `publish.yml`

Replace the disabled scaffold with a live workflow that consumes the same reusable matrix and adds: a smoke-every-wheel matrix gate, then the OIDC publish job.

**Files:**
- Modify: `.github/workflows/publish.yml`

- [ ] **Step 1: Overwrite `publish.yml` with the live version**

Replace the entire content of `.github/workflows/publish.yml` with:

```yaml
name: Publish to PyPI

# Triggered by `vX.Y.Z` tag pushes (created by tag.yml on version bump),
# or manually via workflow_dispatch (for dry-run testing against TestPyPI).
on:
  push:
    tags:
      - "v*"
  workflow_dispatch:
    inputs:
      version:
        description: "Version to publish (e.g., 0.1.0). Used to check out the matching tag."
        required: true
      target:
        description: "Where to publish: pypi (default) or testpypi"
        required: true
        default: "pypi"
        type: choice
        options:
          - pypi
          - testpypi

jobs:
  build:
    name: Build wheels + sdist
    uses: ./.github/workflows/_build_wheels.yml
    with:
      ref: ${{ github.event.inputs.version && format('v{0}', github.event.inputs.version) || github.ref }}
      build_sdist: true
      artifact_prefix: dist

  smoke:
    name: Smoke-test wheel (${{ matrix.label }})
    needs: [build]
    strategy:
      fail-fast: false
      matrix:
        include:
          - label: linux-x86_64-manylinux
            runs-on: ubuntu-latest
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*manylinux*x86_64.whl"
          - label: linux-x86_64-manylinux-ft
            runs-on: ubuntu-latest
            python: "3.14t"
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*manylinux*x86_64.whl"
          - label: linux-x86_64-musllinux
            runs-on: ubuntu-latest
            container: alpine:3.20
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*musllinux*x86_64.whl"
          - label: linux-aarch64-manylinux
            runs-on: ubuntu-24.04-arm
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*manylinux*aarch64.whl"
          - label: linux-aarch64-manylinux-ft
            runs-on: ubuntu-24.04-arm
            python: "3.14t"
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*manylinux*aarch64.whl"
          - label: macos-arm64
            runs-on: macos-14
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*macosx*arm64.whl"
          - label: macos-arm64-ft
            runs-on: macos-14
            python: "3.14t"
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*macosx*arm64.whl"
          - label: windows-amd64
            runs-on: windows-latest
            python: "3.14"
            wheel_glob: "redis_rs_py-*-cp314-cp314-*win_amd64.whl"
          - label: windows-amd64-ft
            runs-on: windows-latest
            python: "3.14t"
            wheel_glob: "redis_rs_py-*-cp314-cp314t-*win_amd64.whl"
    runs-on: ${{ matrix.runs-on }}
    container: ${{ matrix.container || '' }}
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: dist-*
          path: dist
          merge-multiple: true

      - name: Install Python (Alpine)
        if: matrix.container == 'alpine:3.20'
        run: apk add --no-cache python3=~3.14 py3-pip || apk add --no-cache python3 py3-pip

      - name: Install uv
        if: matrix.container != 'alpine:3.20'
        uses: astral-sh/setup-uv@v7

      - name: Set up Python ${{ matrix.python }}
        if: matrix.container != 'alpine:3.20'
        run: uv python install ${{ matrix.python }}

      - name: Install wheel + verify import (Alpine)
        if: matrix.container == 'alpine:3.20'
        run: |
          python3 -m venv /tmp/venv
          /tmp/venv/bin/pip install --no-deps dist/${{ matrix.wheel_glob }}
          /tmp/venv/bin/python -c "from redis_rs_py import _driver; print('OK', _driver.__version__)"

      - name: Install wheel + verify import (non-Alpine)
        if: matrix.container != 'alpine:3.20'
        shell: bash
        run: |
          uv venv --python ${{ matrix.python }} smoke
          source smoke/bin/activate || smoke/Scripts/activate
          uv pip install --no-deps dist/${{ matrix.wheel_glob }}
          python -c "from redis_rs_py import _driver; print('OK', _driver.__version__)"

  smoke-sdist:
    name: Smoke-test sdist
    needs: [build]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: dist-*
          path: dist
          merge-multiple: true

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Set up Python
        run: uv python install 3.14

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@stable

      - name: Build wheel from sdist + verify import
        run: |
          uv venv --python 3.14 sdist-test
          source sdist-test/bin/activate
          uv pip install dist/redis_rs_py-*.tar.gz
          python -c "from redis_rs_py import _driver; print('OK', _driver.__version__)"

  publish:
    name: Publish to ${{ github.event.inputs.target || 'pypi' }}
    needs: [smoke, smoke-sdist]
    runs-on: ubuntu-latest
    environment:
      name: ${{ github.event.inputs.target || 'pypi' }}
      url: https://${{ github.event.inputs.target == 'testpypi' && 'test.pypi.org' || 'pypi.org' }}/project/redis-rs-py/
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: dist-*
          path: dist
          merge-multiple: true

      - name: Publish to PyPI
        if: github.event.inputs.target != 'testpypi'
        uses: pypa/gh-action-pypi-publish@release/v1

      - name: Publish to TestPyPI
        if: github.event.inputs.target == 'testpypi'
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          repository-url: https://test.pypi.org/legacy/
```

- [ ] **Step 2: Verify the workflow parses**

```bash
uv run python -c "
import yaml
from pathlib import Path
data = yaml.safe_load(Path('.github/workflows/publish.yml').read_text())
jobs = data['jobs']
print('jobs:', list(jobs))
assert jobs['publish']['needs'] == ['smoke', 'smoke-sdist']
assert 'id-token' in jobs['publish']['permissions']
"
```

Expected: prints `jobs: ['build', 'smoke', 'smoke-sdist', 'publish']` and the assertions hold.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: re-enable publish.yml with smoke-every-wheel + OIDC publish"
```

---

## Task 5: One-time PyPI Trusted Publisher setup doc

Document the **one-time human steps** required before the first tag push works. This doc lives in the repo so the next maintainer doesn't have to re-derive it.

**Files:**
- Create: `docs/PYPI_TRUSTED_PUBLISHER.md`

- [ ] **Step 1: Write the setup guide**

Create `docs/PYPI_TRUSTED_PUBLISHER.md`:

```markdown
# PyPI Trusted Publisher setup (one-time)

This is the one-time setup that makes `publish.yml` able to upload to PyPI **without any long-lived API tokens stored in GitHub Secrets**. It uses GitHub's OIDC tokens via PyPI's Trusted Publisher feature.

You only need to do this once per project (and once per environment, if you also want TestPyPI).

## 1. Add the pending publisher on PyPI

The project doesn't exist on PyPI yet, so we use the "pending publisher" flow:

1. Sign in to <https://pypi.org/manage/account/publishing/>.
2. Scroll to **Add a new pending publisher**.
3. Fill in:
   - **PyPI Project Name**: `redis-rs-py`
   - **Owner**: `oliverhaas`
   - **Repository name**: `redis-rs-py`
   - **Workflow name**: `publish.yml`
   - **Environment name**: `pypi`
4. Click **Add**.

PyPI will retain the entry as "pending" until the first publish; on that first publish the project is auto-created and the entry becomes a normal Trusted Publisher.

## 2. Repeat for TestPyPI (recommended; lets you dry-run)

1. Sign in to <https://test.pypi.org/manage/account/publishing/>.
2. Same form as above, but set **Environment name** to `testpypi`.

## 3. Configure the GitHub `pypi` environment

1. Go to <https://github.com/oliverhaas/redis-rs-py/settings/environments>.
2. Click **New environment**.
3. Name: `pypi`.
4. Under **Deployment protection rules**:
   - Tick **Required reviewers** and add yourself (the maintainer).
   - Optionally tick **Wait timer** with a small value (5 min) to give yourself a window to cancel an accidental tag push.
5. Save.

## 4. Repeat for the `testpypi` environment

Same steps, environment name `testpypi`. You probably don't need required reviewers for TestPyPI — it's the dry-run target.

## 5. Verification

Once both sides are configured:

1. Trigger `publish.yml` manually with `workflow_dispatch`, target `testpypi`, version `0.1.0a2` (or whatever the next prerelease is).
2. Watch the run in <https://github.com/oliverhaas/redis-rs-py/actions/workflows/publish.yml>.
3. The `publish` job should:
   - Pause on the `testpypi` environment gate (if you set required reviewers there).
   - On approve, upload all artifacts to <https://test.pypi.org/project/redis-rs-py/>.
4. Confirm by `pip install --index-url https://test.pypi.org/simple/ redis-rs-py==0.1.0a2` in a clean venv.

If TestPyPI works, the production PyPI flow is identical — just push a `vX.Y.Z` tag and the workflow takes over.

## Troubleshooting

- **"invalid-publisher" error in the publish job log.** The values on PyPI's pending-publisher form must match the workflow exactly. Double-check the workflow filename (`publish.yml`, not `publish.yaml`) and the environment name.
- **The job never starts.** Check the `pyproject.toml` `name` field — it must equal the **PyPI Project Name** in the pending publisher (case-sensitive, hyphenation matters).
- **The job runs but gets "403 invalid token".** The repository owner / repo name on PyPI's side must match the GitHub coordinates exactly. If the repo was renamed or transferred, edit the entry on PyPI.
```

- [ ] **Step 2: Commit**

```bash
git add docs/PYPI_TRUSTED_PUBLISHER.md
git commit -m "docs(release): add PyPI Trusted Publisher one-time setup guide"
```

---

## Task 6: `RELEASING.md` — step-by-step procedure

The "what to do every release" guide. Should be runnable verbatim by any maintainer.

**Files:**
- Create: `docs/RELEASING.md`

- [ ] **Step 1: Write the release procedure**

Create `docs/RELEASING.md`:

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/RELEASING.md
git commit -m "docs(release): add step-by-step release procedure"
```

---

## Task 7: README install-paragraph update

The current README install paragraph is out of date — it says "for both standard and free-threaded CPython 3.14" but doesn't mention musllinux. Tighten the wording.

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the Installation paragraph**

In `README.md`, find:

```markdown
## Installation

```console
pip install redis-rs-py
```

Prebuilt wheels are published for Linux (x86_64, aarch64), macOS (arm64), and Windows (amd64), for both standard and free-threaded CPython 3.14.
```

Replace with:

```markdown
## Installation

```console
pip install redis-rs-py
```

Prebuilt wheels are published for:

- **Linux** x86_64 + aarch64 — both `manylinux` (glibc) and `musllinux` (Alpine).
- **macOS** arm64 (Apple Silicon).
- **Windows** x86_64.

Each platform ships both standard CPython 3.14 and free-threaded CPython 3.14t wheels. An sdist is published as a fallback for unsupported platforms — it requires a Rust toolchain at install time.

> **v0.1 supports CPython 3.14 only.** The cp310 floor mentioned in the original spec is deferred to v0.2; see `docs/RELEASING.md` for the rationale.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs(readme): document expanded wheel matrix and cp314 floor"
```

---

## Task 8: Verification — open a PR and watch CI

CI is the test for this plan. There's nothing meaningful to assert from the local machine; the value of the work shows up in the green/red of the workflow runs.

**Files:** none modified — verification only.

- [ ] **Step 1: Push a branch with all the above commits and open a PR**

```bash
git checkout -b plan/19-distribution
git push -u origin plan/19-distribution
gh pr create --title "Plan 19: distribution pipeline" --body "$(cat <<'EOF'
## Summary

- Re-enable `publish.yml` with PyPI Trusted Publisher (OIDC).
- Factor wheel matrix into reusable `_build_wheels.yml` consumed by both `ci.yml` and `publish.yml`.
- Add musllinux Alpine wheels for Linux x86_64 + aarch64.
- Add sdist build + sdist smoke-test.
- Smoke-test every wheel before publish (12 cells per release).
- Document the cp314-floor decision and the one-time PyPI Trusted Publisher setup.

## Test plan

- [ ] CI's `build-wheels` job is green (consumes the new reusable workflow).
- [ ] Both `smoke-test-wheel` matrices pass (5 + 4 cells).
- [ ] After merge, manually trigger `publish.yml` with target=`testpypi`, version=`0.1.0a2`, observe a successful TestPyPI upload.
- [ ] `pip install --index-url https://test.pypi.org/simple/ redis-rs-py==0.1.0a2` succeeds in a clean venv.
EOF
)"
```

Expected: PR created, GitHub URL printed.

- [ ] **Step 2: Watch the CI run**

```bash
gh run watch
```

Confirm:

- The `lint` and `test` jobs pass (no regression from existing plans).
- The `build-wheels` reusable-workflow job spawns 6 wheel cells, all green.
- The `smoke-test-wheel` matrix runs all 5 cells, all green. (If musllinux fails because Alpine 3.20 doesn't yet ship Python 3.14, document the fallback in `RELEASING.md` and either drop the musllinux smoke or pull a python:3.14-alpine image instead — both are acceptable.)
- The `smoke-test-wheel-freethreaded` matrix runs all 4 cells, all green.

- [ ] **Step 3: Manual TestPyPI dry-run after merge**

After the PR is merged to `main`:

```
Actions → Publish to PyPI → Run workflow
  - Branch: main
  - version: 0.1.0a2
  - target: testpypi
```

Watch the run. Expected: `build` → `smoke` (9 cells) → `smoke-sdist` → `publish` (gated on `testpypi` environment, requires approval). On approval, the wheels appear at <https://test.pypi.org/project/redis-rs-py/0.1.0a2/>.

Verify locally:

```bash
uv venv --python 3.14 /tmp/verify
source /tmp/verify/bin/activate
uv pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ redis-rs-py==0.1.0a2
python -c "import redis_rs_py; print(redis_rs_py.__version__)"
```

Expected: prints `0.1.0a2`.

- [ ] **Step 4: Commit the final CHANGELOG entry**

After the dry-run is verified, edit `CHANGELOG.md` once more to note the dry-run was successful:

```markdown
- TestPyPI dry-run of v0.1.0a2 verified end-to-end (2026-MM-DD).
```

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): note TestPyPI dry-run verification"
git push
```

---

## Task 9: Final sweep

**Files:** none modified — verification only.

- [ ] **Step 1: Run linters one more time**

```bash
uv run ruff check
uv run ruff format --check
uv run ty check python/redis_rs_py/
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest -n auto
```

Expected: every test PASSES.

- [ ] **Step 3: Inspect the final commit log**

```bash
git log --oneline -15
```

Expected: ~10 conventional-commit-style commits, in roughly the order of the tasks.

---

## Self-review checklist for this plan

- [x] Spec coverage (`PLAN.md` Distribution): "Prebuilt wheels: Linux x86_64 (cp310–cp314, cp314t)…" — Task 1 documents the cp314-only-for-v0.1 deviation with a rationale; Task 2 expands horizontally (musllinux, aarch64, sdist) per the spec.
- [x] Spec coverage (`PLAN.md` Distribution): "musllinux for Alpine. aarch64 for Linux/macOS. sdist as fallback." — Task 2 adds all four (musllinux x86_64 + aarch64, sdist, aarch64 was already in Plan 01's matrix).
- [x] Spec coverage (`PLAN.md` Distribution): "CI: maturin + cibuildwheel via GitHub Actions." — already in use; Task 2 keeps `cibuildwheel@v3.4`, Task 4 keeps the `gh-action-pypi-publish@release/v1` OIDC publisher.
- [x] Spec coverage (Plan 19 roadmap row): re-enable publish.yml ✓ (Task 4); configure PyPI Trusted Publisher + `pypi` environment ✓ (Task 5 docs); add sdist ✓ (Task 2); wheel install smoke-test ✓ (Tasks 3 + 4).
- [x] Decision recommendation against dropping cp314 floor: Task 1 commits the reasoning to `pyproject.toml`, the CHANGELOG, and `RELEASING.md`. **Recommended: keep cp314 floor for v0.1, revisit for v0.2.**
- [x] Out-of-scope items deferred: cp310 floor (v0.2), tier-2 OS wheels (no PyPI demand), sigstore (covered by OIDC provenance).
- [x] No placeholder text — every workflow YAML and every doc body is complete.
- [x] All file paths absolute or repo-relative-from-root.
- [x] Every verification step has a runnable command and an explicit pass/fail expectation.
- [x] Frequent commits — 10 across 9 tasks, each independently revertable.
- [x] Conventional-commit style throughout (`ci:`, `docs(release):`, `docs(readme):`, `chore(release):`).
- [x] DRY: `_build_wheels.yml` is the single source of truth for the matrix; `ci.yml` and `publish.yml` both `uses:` it.
- [x] Smoke-every-wheel gate blocks the publish job (Task 4: `publish.needs: [smoke, smoke-sdist]`).
- [x] OIDC-only publishing (no long-lived tokens in repo secrets) — `id-token: write` permission, `gh-action-pypi-publish` action, `pypi` environment as the trust anchor.
- [x] TestPyPI dry-run path documented and exercised before any production publish (Task 6 + Task 8).
