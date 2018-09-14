#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Runs all tests with Kudu server in docker containers.

set -euo pipefail -x

# http://stackoverflow.com/questions/3572030/bash-script-absolute-path-with-osx
function absolutepath() {
    [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
}

SCRIPT_DIR=$(dirname $(absolutepath "$0"))

PROJECT_ROOT="${SCRIPT_DIR}/../.."

DOCKER_COMPOSE_LOCATION="${SCRIPT_DIR}/docker-compose.yml"


function build_image() {
   docker build -t eskabetxe/kudu ${SCRIPT_DIR}/role

   #docker-compose build -f "${DOCKER_COMPOSE_LOCATION}"
}

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

build_image

start_docker_container

#run product tests
pushd ${PROJECT_ROOT}
set +e
mvn test -pl flink-connector-kudu -P test-kudu
EXIT_CODE=$?
set -e
popd

cleanup_docker_container

exit ${EXIT_CODE}