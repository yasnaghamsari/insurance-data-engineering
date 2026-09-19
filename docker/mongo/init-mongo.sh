#!/bin/bash
# Local Docker Compose seed for the "claims" source collection.
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
