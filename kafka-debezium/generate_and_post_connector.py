import os
import requests
import json

# Load variables from kafka-debezium/.env into the process environment.
# This file holds Postgres connection details (host, port, user, password, db).
# Without this call, all os.getenv() calls below would return None.
from dotenv import load_dotenv
load_dotenv()

# -------------------------------------------------------------
# WHAT IS DEBEZIUM?
# Debezium is a Change Data Capture (CDC) tool. It watches the
# PostgreSQL Write-Ahead Log (WAL) — the internal log Postgres
# writes before committing any INSERT/UPDATE/DELETE — and
# republishes every change as a JSON event on a Kafka topic.
# This means downstream systems (MinIO consumer, Snowflake)
# learn about every database change in real time, without
# polling the database.
# -------------------------------------------------------------

# -------------------------------------------------------------
# CONNECTOR CONFIGURATION
# A "connector" is a named Debezium job that:
#   1. Connects to one PostgreSQL database
#   2. Reads its WAL via a replication slot
#   3. Publishes change events to Kafka topics
#
# The config below is sent as JSON to the Kafka Connect REST
# API. Kafka Connect then manages the connector as a long-
# running process inside the Docker container.
# -------------------------------------------------------------
connector_config = {
    # Unique name for this connector inside Kafka Connect.
    # You can query its status later at:
    #   GET http://localhost:8083/connectors/banking-postgres-connector/status
    "name": "banking-postgres-connector",

    "config": {
        # Tells Kafka Connect which Debezium plugin to load.
        # This one is specifically for PostgreSQL.
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # --- PostgreSQL connection details ---
        # These come from kafka-debezium/.env.
        # IMPORTANT: when running inside Docker, POSTGRES_HOST should be
        # the Docker service name "postgres" (not "localhost"), because
        # Debezium runs inside the connect container on the same Docker
        # network as the postgres container.
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port":     os.getenv("POSTGRES_PORT"),
        "database.user":     os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname":   os.getenv("POSTGRES_DB"),

        # Logical name for this database server. Debezium uses this
        # as a prefix when naming Kafka topics. For example:
        #   banking_server.public.customers
        #   banking_server.public.accounts
        #   banking_server.public.transactions
        # It does NOT need to match the actual Postgres server hostname.
        "database.server.name": "banking",

        # WAL decoding plugin installed on the Postgres server.
        # "pgoutput" is built into Postgres 10+ so no extra extension
        # is required. The alternative is "decoderbufs" (needs an
        # extension). The Postgres container in docker-compose.yml
        # is already configured with wal_level=logical to support this.
        "plugin.name": "pgoutput",

        # Replication slot name on the Postgres server.
        # A replication slot is a cursor that tracks how far Debezium
        # has read the WAL. It guarantees Debezium won't miss any
        # changes even if it restarts.
        # WARNING: if you delete the connector without dropping this
        # slot, Postgres will keep accumulating WAL forever. Drop it
        # with: SELECT pg_drop_replication_slot('banking_slot');
        "slot.name": "banking_slot",

        # "filtered" means Debezium will only replicate tables that
        # it has been explicitly told to watch (or all tables if no
        # filter is set). The alternative "all_tables" creates a
        # publication covering every table in the database.
        "publication.autocreate.mode": "filtered",

        # When a row is deleted, Postgres CDC emits two events:
        # a DELETE event followed by a tombstone (a message with a
        # null payload that signals Kafka log compaction to remove
        # the key). Setting this to "false" suppresses the tombstone,
        # which simplifies downstream consumers that don't need it.
        "tombstones.on.delete": "false",

        # Prefix applied to every Kafka topic this connector creates.
        # Combined with database.server.name the full topic name becomes:
        #   {topic.prefix}.{schema}.{table}
        #   → banking.public.customers
        # Note: some Debezium versions use topic.prefix; others use
        # database.server.name. Both are set here for compatibility.
        "topic.prefix": "banking",

        # How to represent Postgres NUMERIC/DECIMAL columns in the
        # Kafka messages (e.g. amount, balance).
        #   "precise" (default) → Base64-encoded bytes (unusable as numbers)
        #   "double"            → normal floating-point numbers like 1234.56
        #   "string"            → decimal strings like "1234.56"
        # We use "double" so downstream consumers (MinIO → Snowflake)
        # receive real numbers that can be summed and compared.
        "decimal.handling.mode": "double"
    }
}

# -------------------------------------------------------------
# KAFKA CONNECT REST API
# Kafka Connect exposes an HTTP API on port 8083 (mapped from
# the "connect" Docker container). Sending a POST to /connectors
# registers a new connector and starts it immediately.
#
# Useful endpoints to know:
#   GET  /connectors                          → list all connectors
#   GET  /connectors/{name}/status            → check if RUNNING/FAILED
#   DELETE /connectors/{name}                 → stop and remove
#   PUT  /connectors/{name}/config            → update config live
# -------------------------------------------------------------
DEBEZIUM_URL = "http://localhost:8083/connectors"

response = requests.post(
    DEBEZIUM_URL,
    headers={"Content-Type": "application/json"},
    data=json.dumps(connector_config)
)

# 201 Created  → connector registered for the first time
# 200 OK       → connector already existed and was updated
# 409 Conflict → connector already exists (run DELETE first to recreate)
# 5xx          → Kafka Connect or Postgres is not reachable yet
if response.status_code in [200, 201]:
    print("✅ Connector created successfully!")
    print(f"   Monitor status at: {DEBEZIUM_URL}/banking-postgres-connector/status")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")
