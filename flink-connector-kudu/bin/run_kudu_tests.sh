#!/bin/bash
#
# Runs all tests with Kudu server in docker containers.
#
# ./run_kudu_tests.sh <nodes> <inferSchemaPrefix>
#
# <nodes>                Number of tablet servers (1 oder 3). Also used for number of replicas.
# <inferSchemaPrefix>    InferSchema setup: "null" = disabled,
#                                           ""     = enabled, using empty prefix,
#                                           "presto::" = enabled, using standard prefix
#
# If arguments are missing, defaults are 1 tablet server and disabled inferSchema.

set -euo pipefail -x

export KUDU_MASTER_RPC_PORT=17051
export DOCKER_HOST_IP=172.25.0.5
#export DOCKER_HOST_IP=$(docker network inspect bridge --format='{{index .IPAM.Config 0 "Gateway"}}')

# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
function absolutepath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

SCRIPT_DIR=$(dirname $(absolutepath "$0"))

PROJECT_ROOT="${SCRIPT_DIR}/../.."

# single or three tablet nodes?
if [ $# -eq 0 ]
then
  DOCKER_COMPOSE_LOCATION="${SCRIPT_DIR}/../conf/docker-compose-single-node.yaml"
elif [ $1 -eq 1 ]
then
  DOCKER_COMPOSE_LOCATION="${SCRIPT_DIR}/../conf/docker-compose-single-node.yaml"
elif [ $1 -eq 3 ]
then
  DOCKER_COMPOSE_LOCATION="${SCRIPT_DIR}/../conf/docker-compose-three-nodes.yaml"
else
  echo unknown node configuration
  exit 1
fi

# emulate schemas ?
if [ $# -lt 2 ]
then
  TEST_SCHEMA_EMULATION_PREFIX=null
else
  TEST_SCHEMA_EMULATION_PREFIX=$2
fi

function start_docker_container() {
  # stop already running containers
  #docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down || true

  # start containers
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" up -d
}

function cleanup_docker_container() {
  docker-compose -f "${DOCKER_COMPOSE_LOCATION}" down
  #true
}


start_docker_container

# run product tests
pushd ${PROJECT_ROOT}
set +e
mvn -pl flink-connector-kudu test \
  -Dkudu.client.master-addresses=${DOCKER_HOST_IP}:${KUDU_MASTER_RPC_PORT} \
  -Dkudu.schema-emulation.prefix=${TEST_SCHEMA_EMULATION_PREFIX}
EXIT_CODE=$?
set -e
popd

cleanup_docker_container

exit ${EXIT_CODE}