#!/usr/bin/env bash
set -euo pipefail

# Supported Airflow version: >=3.1
# (SimpleAuthManager + api-server/webserver fallback handling)

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_HOME="${PROJECT_ROOT}/.airflow"
DAGS_FOLDER="${PROJECT_ROOT}/tests/weaver/dags"
VENV_ACTIVATE="${PROJECT_ROOT}/.venv/bin/activate"
PID_DIR="${AIRFLOW_HOME}/pids"
LOG_DIR="${AIRFLOW_HOME}/logs"
API_PID_FILE="${PID_DIR}/airflow-api-server.pid"
SCHED_PID_FILE="${PID_DIR}/airflow-scheduler.pid"
DAGPROC_PID_FILE="${PID_DIR}/airflow-dag-processor.pid"

if [ ! -f "${VENV_ACTIVATE}" ]; then
  echo "Virtual environment not found: ${VENV_ACTIVATE}"
  echo "Create/install first: python -m venv .venv && .venv/bin/pip install -e \".[airflow,dev]\""
  exit 1
fi
source "${VENV_ACTIVATE}"

mkdir -p "${AIRFLOW_HOME}" "${PID_DIR}" "${LOG_DIR}"

export AIRFLOW_HOME
export AIRFLOW__CORE__DAGS_FOLDER="${DAGS_FOLDER}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH="${PROJECT_ROOT}/src:${PROJECT_ROOT}:${PYTHONPATH:-}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
AIRFLOW_UI_USERNAME="${AIRFLOW_UI_USERNAME:-admin}"
AIRFLOW_UI_PASSWORD="${AIRFLOW_UI_PASSWORD:-admin}"
AIRFLOW_PASSWORDS_FILE="${AIRFLOW_HOME}/simple_auth_manager_passwords.json"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${AIRFLOW_UI_USERNAME}:admin"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="${AIRFLOW_PASSWORDS_FILE}"

if [ ! -f "${AIRFLOW_HOME}/airflow.db" ]; then
  airflow db migrate || airflow db init
fi

cat > "${AIRFLOW_PASSWORDS_FILE}" <<EOF
{"${AIRFLOW_UI_USERNAME}":"${AIRFLOW_UI_PASSWORD}"}
EOF

if airflow api-server --help >/dev/null 2>&1; then
  API_SERVICE_NAME="airflow api-server"
  API_CMD=(airflow api-server --port 8080)
else
  API_SERVICE_NAME="airflow webserver"
  API_CMD=(airflow webserver --port 8080)
fi

if [ -f "${API_PID_FILE}" ] && kill -0 "$(cat "${API_PID_FILE}")" 2>/dev/null; then
  echo "${API_SERVICE_NAME} already running (pid $(cat "${API_PID_FILE}"))."
else
  nohup "${API_CMD[@]}" > "${LOG_DIR}/api-server.out" 2>&1 &
  echo $! > "${API_PID_FILE}"
  echo "Started ${API_SERVICE_NAME} (pid $(cat "${API_PID_FILE}"))."
fi

if [ -f "${SCHED_PID_FILE}" ] && kill -0 "$(cat "${SCHED_PID_FILE}")" 2>/dev/null; then
  echo "Airflow scheduler already running (pid $(cat "${SCHED_PID_FILE}"))."
else
  nohup airflow scheduler > "${LOG_DIR}/scheduler.out" 2>&1 &
  echo $! > "${SCHED_PID_FILE}"
  echo "Started Airflow scheduler (pid $(cat "${SCHED_PID_FILE}"))."
fi

if [ -f "${DAGPROC_PID_FILE}" ] && kill -0 "$(cat "${DAGPROC_PID_FILE}")" 2>/dev/null; then
  echo "Airflow dag-processor already running (pid $(cat "${DAGPROC_PID_FILE}"))."
else
  nohup airflow dag-processor > "${LOG_DIR}/dag-processor.out" 2>&1 &
  echo $! > "${DAGPROC_PID_FILE}"
  echo "Started Airflow dag-processor (pid $(cat "${DAGPROC_PID_FILE}"))."
fi

echo "AIRFLOW_HOME=${AIRFLOW_HOME}"
echo "DAGS_FOLDER=${DAGS_FOLDER}"
echo "Open Airflow UI at: http://localhost:8080"
echo "Airflow credentials: ${AIRFLOW_UI_USERNAME}/${AIRFLOW_UI_PASSWORD}"
