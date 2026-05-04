#!/usr/bin/env python3
"""Render the compatibility matrix into README.md.

Run modes:
    render          — overwrite the block, exit 0.
    check           — diff the rendered block against the README; exit 1
                      if it would change. Used by the pre-commit hook.

The block lives between two HTML comment markers in README.md. Anything
between the markers is regenerated; anything outside is left alone.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"
START = "<!-- compat:start -->"
END = "<!-- compat:end -->"

# So `python scripts/render_compat_matrix.py` works without invoking via uv.
sys.path.insert(0, str(REPO_ROOT))

from tests._compat_manifest import MANIFEST, ManifestEntry  # noqa: E402

STATUS_BADGE = {
    "implemented": "[*] implemented",
    "partial": "[~] partial",
    "deferred": "[ ] deferred",
}

FAMILY_ORDER = [
    "strings",
    "lists",
    "hashes",
    "sets",
    "zsets",
    "streams",
    "scripts",
    "admin",
    "pubsub",
    "transactions",
    "cluster",
    "sentinel",
]


def _by_family() -> dict[str, list[ManifestEntry]]:
    out: dict[str, list[ManifestEntry]] = defaultdict(list)
    for entry in MANIFEST:
        out[entry["family"]].append(entry)
    return out


def _render_family_table(entries: list[ManifestEntry]) -> str:
    rows = ["| Command | Method | Status | Since | Notes |", "|---|---|---|---|---|"]
    rows.extend(
        f"| `{entry['command']}` | `{entry['method']}` | "
        f"{STATUS_BADGE[entry['status']]} | "
        f"{entry['since_redis']} | {entry['notes']} |"
        for entry in sorted(entries, key=lambda e: e["command"])
    )
    return "\n".join(rows)


def _render_summary(grouped: dict[str, list[ManifestEntry]]) -> str:
    total = sum(len(v) for v in grouped.values())
    impl = sum(1 for v in grouped.values() for e in v if e["status"] == "implemented")
    part = sum(1 for v in grouped.values() for e in v if e["status"] == "partial")
    defr = sum(1 for v in grouped.values() for e in v if e["status"] == "deferred")
    return f"**{total} commands tracked** — {impl} implemented, {part} partial, {defr} deferred."


def render() -> str:
    grouped = _by_family()
    parts: list[str] = [_render_summary(grouped), ""]
    for family in FAMILY_ORDER:
        if family not in grouped:
            continue
        parts.append(f"### {family.title()}")
        parts.append("")
        parts.append(_render_family_table(grouped[family]))
        parts.append("")
    # Any family not in FAMILY_ORDER shows up at the end so we can't lose entries.
    leftover = sorted(set(grouped) - set(FAMILY_ORDER))
    for family in leftover:
        parts.append(f"### {family.title()}")
        parts.append("")
        parts.append(_render_family_table(grouped[family]))
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def _splice(readme_text: str, block: str) -> str:
    if START not in readme_text or END not in readme_text:
        raise SystemExit(
            f"README.md is missing the {START} / {END} markers; "
            f"add them between the Benchmarks and Quickstart sections.",
        )
    pre, _, rest = readme_text.partition(START)
    _, _, post = rest.partition(END)
    return f"{pre}{START}\n\n{block}\n{END}{post}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["render", "check"], default="render", nargs="?")
    args = parser.parse_args()

    block = render()
    current = README.read_text()
    rendered = _splice(current, block)

    if args.mode == "check":
        if current != rendered:
            sys.stderr.write(
                "README compatibility matrix is stale.\nRun: uv run python scripts/render_compat_matrix.py render\n",
            )
            return 1
        return 0

    if current != rendered:
        README.write_text(rendered)
        sys.stdout.write(f"updated {README}\n")
    else:
        sys.stdout.write(f"{README} already up to date\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
