#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
for script in \
  scripts/integration.sh \
  scripts/data-integrity.sh \
  scripts/membership-smoke.sh \
  scripts/replication-smoke.sh \
  scripts/controller-election-smoke.sh \
  scripts/controller-metadata-smoke.sh \
  scripts/partition-leader-failover-smoke.sh \
  scripts/reassignment-smoke.sh \
  scripts/crash-restart.sh
do
  echo "===== $script ====="
  bash "$script"
done
echo "All MessLessMQ multi-process and recovery smoke tests passed."
