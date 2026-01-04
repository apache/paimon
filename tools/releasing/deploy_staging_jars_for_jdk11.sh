#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="${BASE_DIR}/../../"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

###########################

cd ${PROJECT_ROOT}

echo "Building flink2 and iceberg modules"
${MVN} clean install -Pdocs-and-source,flink2 -DskipTests \
-pl org.apache.paimon:paimon-flink-2.0,org.apache.paimon:paimon-flink-2.1,org.apache.paimon:paimon-flink-2.2,org.apache.paimon:paimon-iceberg -am $CUSTOM_OPTIONS

echo "Deploying flink2 and iceberg modules to repository.apache.org"
${MVN} deploy -Papache-release,docs-and-source,flink2 -DskipTests -DretryFailedDeploymentCount=10 \
-pl org.apache.paimon:paimon-flink-2.0,org.apache.paimon:paimon-flink-2.1,org.apache.paimon:paimon-flink-2.2,org.apache.paimon:paimon-flink2-common,org.apache.paimon:paimon-iceberg $CUSTOM_OPTIONS

cd ${CURR_DIR}
