#!/bin/sh
set -e

echo "==================================================================="
echo "Seeding PostgreSQL database from development container..."
echo "==================================================================="
echo "Host: ${POSTGRES_HOST}"
echo "Port: ${POSTGRES_PORT}"
echo "Database: ${POSTGRES_DB}"
echo "User: ${POSTGRES_USER}"
echo "SSL Mode: ${POSTGRES_SSL_MODE:-disable}"
echo "Environment: ${ENVIRONMENT:-}"
echo "==================================================================="

export PGPASSWORD="${POSTGRES_PASSWORD}"
export PGSSLMODE="${POSTGRES_SSL_MODE}"
if [ -n "${POSTGRES_SSL_MODE}" ]; then
  export PGSSLMODE="${POSTGRES_SSL_MODE}"
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Wait for postgres to be ready to accept connections
echo "Waiting for PostgreSQL to be ready..."
until psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c '\q' 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done
echo "PostgreSQL is ready!"

# In local environment, tables are already created by docker-entrypoint-initdb.d scripts
# In perf-test environment, tables exist but need to be truncated
if [ "${ENVIRONMENT:-}" = "perf-test" ]; then
  echo "Perf-test environment detected - truncating existing data from tables..."
  echo "Executing 00-truncate-tables.sql..."
  psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 \
    -f "${SCRIPT_DIR}/00-truncate-tables.sql"
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to truncate tables"
    exit 1
  fi
fi

# Always seed the database with test data
echo "Seeding database with test data..."
echo "Executing 02-seed-postgres.sql..."
psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -f "${SCRIPT_DIR}/02-seed-postgres.sql"
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to seed database with test data"
  exit 1
fi

echo "==================================================================="
echo "PostgreSQL seeding completed successfully"
echo "==================================================================="

unset PGPASSWORD
unset PGSSLMODE
