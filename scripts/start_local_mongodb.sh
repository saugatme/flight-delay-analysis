#!/usr/bin/env bash
set -euo pipefail

mongodb_home="${MONGODB_HOME:-${HOME}/opt/mongodb-8.0.28}"
mongodb_data="${MONGODB_DATA:-${HOME}/.local/share/flight-delay-mongodb}"
mongodb_state="${MONGODB_STATE:-${HOME}/.local/state/flight-delay-mongodb}"

mkdir -p "${mongodb_data}" "${mongodb_state}"

authentication=(--auth)
if [[ "${1:-}" == "--no-auth" ]]; then
    authentication=()
fi

if "${mongodb_home}/bin/mongod" \
    --dbpath "${mongodb_data}" \
    --bind_ip 127.0.0.1 \
    --port 27017 \
    --logpath "${mongodb_state}/mongod.log" \
    --pidfilepath "${mongodb_state}/mongod.pid" \
    "${authentication[@]}" \
    --fork; then
    echo "MongoDB started on 127.0.0.1:27017"
else
    echo "MongoDB did not start. Inspect ${mongodb_state}/mongod.log" >&2
    exit 1
fi
