#!/usr/bin/env bash
set -euo pipefail

# Companion script for `start_airflow.sh` (Airflow >=3.1 local runtime).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/stop_airflow.sh"
"${SCRIPT_DIR}/start_airflow.sh"
