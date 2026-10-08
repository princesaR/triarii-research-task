#!/bin/sh
# Runs once, on the first start of an empty data volume (docker-entrypoint-initdb.d).
# One database and one login role per service; neither service uses the superuser.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v ingest_pw="$INGEST_DB_PASSWORD" -v alerts_pw="$ALERTS_DB_PASSWORD" <<'SQL'
CREATE ROLE ingest LOGIN PASSWORD :'ingest_pw';
CREATE ROLE alerts LOGIN PASSWORD :'alerts_pw';

-- Each role owns its own database, so it can create its tables there and nowhere else.
CREATE DATABASE ingest_db OWNER ingest;
CREATE DATABASE alerts_db OWNER alerts;

-- By default every role may connect to every database; close that.
REVOKE CONNECT ON DATABASE ingest_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE alerts_db FROM PUBLIC;
SQL
