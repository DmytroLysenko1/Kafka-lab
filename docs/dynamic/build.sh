#!/usr/bin/env bash
# Inline walkthrough.css / walkthrough.js / steps.js into standalone single-file pages.
#
# The modular sources here are what GitHub Pages serves and what gets reviewed.
# dist/ exists only for handing someone one file — a Slack attachment, or an
# offline copy on a laptop before a talk.
#
# Usage: docs/dynamic/build.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/dist"
rm -rf "$out"
mkdir -p "$out"

inline() {
  python3 - "$1" "$2" <<'PY'
import pathlib, re, sys

src = pathlib.Path(sys.argv[1])
dst = pathlib.Path(sys.argv[2])
base = src.parent
html = src.read_text(encoding="utf-8")

def read(ref):
    return (base / ref).resolve().read_text(encoding="utf-8")

html = re.sub(r'<link rel="stylesheet" href="([^"]+)">',
              lambda m: "<style>\n" + read(m.group(1)) + "</style>", html)
html = re.sub(r'<script src="([^"]+)"></script>',
              lambda m: "<script>\n" + read(m.group(1)) + "</script>", html)
html = re.sub(r'<p class="wrap backlink">.*?</p>\n?', "", html, flags=re.S)

dst.write_text(html, encoding="utf-8")
print("  " + dst.name)
PY
}

for page in "$here"/*/index.html; do
  name="$(basename "$(dirname "$page")")"
  [ "$name" = "dist" ] && continue
  inline "$page" "$out/$name.html"
done

inline "$here/index.html" "$out/index.html"

echo "standalone pages written to docs/dynamic/dist/"
