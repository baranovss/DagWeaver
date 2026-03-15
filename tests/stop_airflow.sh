#!/usr/bin/env bash
set -euo pipefail

# Companion script for `start_airflow.sh` (Airflow >=3.1 local runtime).

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_HOME="${PROJECT_ROOT}/.airflow"
PID_DIR="${AIRFLOW_HOME}/pids"
API_PID_FILE="${PID_DIR}/airflow-api-server.pid"
LEGACY_WEB_PID_FILE="${PID_DIR}/airflow-webserver.pid"
SCHED_PID_FILE="${PID_DIR}/airflow-scheduler.pid"
DAGPROC_PID_FILE="${PID_DIR}/airflow-dag-processor.pid"

stop_by_pid_file() {
  local pid_file="$1"
  local service_name="$2"

  if [ ! -f "${pid_file}" ]; then
    echo "${service_name} pid file not found, skipping."
    return
  fi

  local pid
  pid="$(cat "${pid_file}")"

  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}"
    echo "Stopped ${service_name} (pid ${pid})."
  else
    echo "${service_name} is not running (stale pid ${pid})."
  fi

  rm -f "${pid_file}"
}

stop_by_pid_file "${DAGPROC_PID_FILE}" "airflow dag-processor"
stop_by_pid_file "${SCHED_PID_FILE}" "airflow scheduler"
stop_by_pid_file "${API_PID_FILE}" "airflow api-server"
stop_by_pid_file "${LEGACY_WEB_PID_FILE}" "airflow webserver"
