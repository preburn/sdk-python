#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: scripts/check-dco.sh <base-revision> <head-revision>" >&2
  exit 2
fi
base_revision="$1"
head_revision="$2"

unsigned_commits="$(
  git log --no-merges --format='%h%x09%(trailers:key=Signed-off-by,valueonly,separator=%x2C)%x09%s' "${base_revision}..${head_revision}" \
    | awk -F '\t' '$2 == "" { print $1, $3 }'
)"
if [[ -n "${unsigned_commits}" ]]; then
  echo "${unsigned_commits}"
  echo "commits above have no Signed-off-by trailer, see CONTRIBUTING.md" >&2
  exit 1
fi
