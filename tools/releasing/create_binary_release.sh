#!/usr/bin/env bash

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

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

if [ -z "${RELEASE_VERSION:-}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "tools" ]] ; then
  echo "You have to call the script from the tools/ dir"
  exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

cd ..

FLINK_DIR=`pwd`
RELEASE_DIR=${FLINK_DIR}/tools/releasing/release

rm -rf ${RELEASE_DIR}
mkdir ${RELEASE_DIR}

###########################

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -DskipTests
cp paimon-flink/paimon-flink-1.17/target/paimon-flink-1.17-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-flink/paimon-flink-1.16/target/paimon-flink-1.16-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-flink/paimon-flink-1.15/target/paimon-flink-1.15-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-flink/paimon-flink-1.14/target/paimon-flink-1.14-${RELEASE_VERSION}.jar ${RELEASE_DIR}

cp paimon-hive/paimon-hive-catalog/target/paimon-hive-catalog-${RELEASE_VERSION}.jar ${RELEASE_DIR}

cp paimon-hive/paimon-hive-connector-3.1/target/paimon-hive-connector-3.1-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-hive/paimon-hive-connector-2.3/target/paimon-hive-connector-2.3-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-hive/paimon-hive-connector-2.2/target/paimon-hive-connector-2.2-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-hive/paimon-hive-connector-2.1/target/paimon-hive-connector-2.1-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-hive/paimon-hive-connector-2.1-cdh-6.3/target/paimon-hive-connector-2.1-cdh-6.3-${RELEASE_VERSION}.jar ${RELEASE_DIR}

cp paimon-spark/paimon-spark-3.3/target/paimon-spark-3.3-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-spark/paimon-spark-3.2/target/paimon-spark-3.2-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-spark/paimon-spark-3.1/target/paimon-spark-3.1-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp paimon-spark/paimon-spark-2/target/paimon-spark-2-${RELEASE_VERSION}.jar ${RELEASE_DIR}

cd ${RELEASE_DIR}
gpg --armor --detach-sig "paimon-flink-1.17-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-flink-1.16-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-flink-1.15-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-flink-1.14-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-catalog-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-connector-3.1-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-connector-2.3-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-connector-2.2-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-connector-2.1-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-hive-connector-2.1-cdh-6.3-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-spark-3.3-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-spark-3.2-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-spark-3.1-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "paimon-spark-2-${RELEASE_VERSION}.jar"

$SHASUM "paimon-flink-1.17-${RELEASE_VERSION}.jar" > "paimon-flink-1.17-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-flink-1.16-${RELEASE_VERSION}.jar" > "paimon-flink-1.16-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-flink-1.15-${RELEASE_VERSION}.jar" > "paimon-flink-1.15-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-flink-1.14-${RELEASE_VERSION}.jar" > "paimon-flink-1.14-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-catalog-${RELEASE_VERSION}.jar" > "paimon-hive-catalog-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-connector-3.1-${RELEASE_VERSION}.jar" > "paimon-hive-connector-3.1-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-connector-2.3-${RELEASE_VERSION}.jar" > "paimon-hive-connector-2.3-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-connector-2.2-${RELEASE_VERSION}.jar" > "paimon-hive-connector-2.2-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-connector-2.1-${RELEASE_VERSION}.jar" > "paimon-hive-connector-2.1-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-hive-connector-2.1-cdh-6.3-${RELEASE_VERSION}.jar" > "paimon-hive-connector-2.1-cdh-6.3-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-spark-3.3-${RELEASE_VERSION}.jar" > "paimon-spark-3.3-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-spark-3.2-${RELEASE_VERSION}.jar" > "paimon-spark-3.2-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-spark-3.1-${RELEASE_VERSION}.jar" > "paimon-spark-3.1-${RELEASE_VERSION}.jar.sha512"
$SHASUM "paimon-spark-2-${RELEASE_VERSION}.jar" > "paimon-spark-2-${RELEASE_VERSION}.jar.sha512"
