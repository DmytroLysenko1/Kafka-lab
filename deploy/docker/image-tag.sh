#!/usr/bin/env bash
# Prints the image tag for the working tree: the commit when the tree is clean, and the
# commit plus a hash of the uncommitted changes (tracked diffs and untracked files alike)
# when it is not. "abc123-dirty" alone would name every dirty tree on that commit the same,
# and a rebuilt tag changes no pod template, so nothing would roll. The hash covers every
# file, docs included: editing a document also gives the next build a new tag — the price of
# a tag that never lies about what is in the image.
set -euo pipefail
cd "$(dirname "$0")/../.."

commit=$(git rev-parse --short HEAD)
if [ -z "$(git status --porcelain)" ]; then
  echo "$commit"
  exit 0
fi
changes=$( { git diff HEAD --binary; git ls-files --others --exclude-standard -z | xargs -0 cat 2>/dev/null; } | shasum | cut -c1-8)
echo "$commit-dirty-$changes"
