#!/usr/bin/env bash
# Fails if total statement coverage in the profile is below THRESHOLD.
set -euo pipefail

PROFILE="${1:-cover.out}"
THRESHOLD="${2:-85}"

total="$(go tool cover -func="${PROFILE}" | awk '/^total:/ {sub(/%/, "", $3); print $3}')"
echo "Total coverage: ${total}% (threshold ${THRESHOLD}%)"

awk -v t="${total}" -v th="${THRESHOLD}" 'BEGIN { if (t + 0 < th + 0) { exit 1 } }' || {
  echo "::error::coverage ${total}% is below the ${THRESHOLD}% threshold"
  exit 1
}
