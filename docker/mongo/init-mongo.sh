#!/bin/bash
# Local Docker Compose seed for the "claims" source collection, read by
# Bronze via pymongo when SOURCE_MODE=databases (the Airflow DAG's setting).
# It loads data/samples/mongodb/claims.json — the same file Bronze reads
# directly when SOURCE_MODE=files — so both modes see the same claims.
#
# setup/mongodb/config.sh targets an Atlas `mongodb+srv://` connection
# string and isn't usable against a local container, so it's kept as-is as
# the cloud provisioning reference; this script is the local-compose
# equivalent, run automatically by the mongo image's
# docker-entrypoint-initdb.d mechanism.
set -euo pipefail

mongoimport \
    --db insurance \
    --collection claims \
    --file /docker-entrypoint-initdb.d/claims.json \
    --jsonArray

# The mongo image runs init scripts against a temporary mongod that already
# answers pings, so "is the server up" isn't the same as "is the seed
# loaded". docker-compose.yml's healthcheck waits for this marker instead.
mongosh --quiet insurance --eval 'db.seed_status.insertOne({ claims_loaded: true })'
