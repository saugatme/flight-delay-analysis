#!/usr/bin/env bash
set -euo pipefail

mongodb_home="${MONGODB_HOME:-${HOME}/opt/mongodb-8.0.28}"
mongodb_data="${MONGODB_DATA:-${HOME}/.local/share/flight-delay-mongodb}"

"${mongodb_home}/bin/mongod" --shutdown --dbpath "${mongodb_data}"
