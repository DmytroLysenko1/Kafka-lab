#!/usr/bin/env bash
# Runs every question in questions.tsv through askdocs and prints a log for results/.
#   run.sh retrieve   the evidence each question gets, and whether it holds the labelled
#                     document (recall@6); no model, no key
#   run.sh answer     the model's answers, with tokens, stop reason, exit code and time; needs
#                     ANTHROPIC_API_KEY in the environment, which only the SDK reads — it is
#                     never printed, so the log can be committed
set -uo pipefail
mode=${1:?usage: run.sh retrieve|answer}
here=$(cd "$(dirname "$0")" && pwd)
binary="$here/../bin/askdocs"
root="$here/../.."

# The results directory is left out of the check: make has already truncated the log this
# run writes into, so counting it would mark every run as made from uncommitted changes.
echo "# askdocs eval ($mode), $(date -u +%Y-%m-%dT%H:%MZ), commit $(git -C "$root" rev-parse --short HEAD)$(git -C "$root" diff --quiet -- . ':!askdocs/results' || echo ' + uncommitted changes')"
asked=0 found=0
while IFS=$'\t' read -r question expected; do
  [[ -z "$question" || "$question" == \#* ]] && continue
  echo
  echo "\$ askdocs $([[ $mode == retrieve ]] && echo '-retrieve-only ')\"$question\""
  if [[ $mode == retrieve ]]; then
    output=$("$binary" -root "$root" -retrieve-only "$question" 2>&1); code=$?
    printf '%s\nexit=%d\n' "$output" "$code"
    if [[ $expected == - ]]; then
      verdict=$([[ $code == 66 ]] && echo "hit: nothing retrieved, as labelled" || echo "miss: something was retrieved for a question the docs do not cover")
    else
      asked=$((asked + 1))
      if grep -q -- "^- $expected" <<<"$output"; then
        found=$((found + 1)); verdict="hit: $expected"
      else
        verdict="miss: $expected is not in the evidence"
      fi
    fi
    echo "$verdict"
  else
    started=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
    "$binary" -root "$root" "$question" 2>&1; code=$?
    finished=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
    echo "exit=$code, $(perl -e "printf '%.1f', $finished - $started") s"
  fi
done < "$here/questions.tsv"
if [[ $mode == retrieve ]]; then
  echo
  echo "recall@6: $found of $asked labelled documents were in their question's evidence"
fi
