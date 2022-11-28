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
cp flink-table-store-dist/target/flink-table-store-dist-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-catalog-${RELEASE_VERSION}_2.3.jar
cp flink-table-store-hive/flink-table-store-hive-connector/target/flink-table-store-hive-connector-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-connector-${RELEASE_VERSION}_2.3.jar
cp flink-table-store-spark/target/flink-table-store-spark-${RELEASE_VERSION}.jar ${RELEASE_DIR}
cp flink-table-store-spark2/target/flink-table-store-spark2-${RELEASE_VERSION}.jar ${RELEASE_DIR}

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -DskipTests -Phive-3.1 -f flink-table-store-hive
cp flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-catalog-${RELEASE_VERSION}_3.1.jar
cp flink-table-store-hive/flink-table-store-hive-connector/target/flink-table-store-hive-connector-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-connector-${RELEASE_VERSION}_3.1.jar

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -DskipTests -Phive-2.2 -f flink-table-store-hive
cp flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-catalog-${RELEASE_VERSION}_2.2.jar
cp flink-table-store-hive/flink-table-store-hive-connector/target/flink-table-store-hive-connector-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-connector-${RELEASE_VERSION}_2.2.jar

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -DskipTests -Phive-2.1 -f flink-table-store-hive
cp flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-catalog-${RELEASE_VERSION}_2.1.jar
cp flink-table-store-hive/flink-table-store-hive-connector/target/flink-table-store-hive-connector-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-hive-connector-${RELEASE_VERSION}_2.1.jar

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -Dmaven.test.skip=true -Pflink-1.15
cp flink-table-store-dist/target/flink-table-store-dist-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-dist-${RELEASE_VERSION}_1.15.jar

mvn clean install -Dcheckstyle.skip=true -Dgpg.skip -Dmaven.test.skip=true -Pflink-1.14
cp flink-table-store-dist/target/flink-table-store-dist-${RELEASE_VERSION}.jar ${RELEASE_DIR}/flink-table-store-dist-${RELEASE_VERSION}_1.14.jar

cd ${RELEASE_DIR}
gpg --armor --detach-sig "flink-table-store-dist-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "flink-table-store-hive-catalog-${RELEASE_VERSION}_3.1.jar"
gpg --armor --detach-sig "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.3.jar"
gpg --armor --detach-sig "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.2.jar"
gpg --armor --detach-sig "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.1.jar"
gpg --armor --detach-sig "flink-table-store-hive-connector-${RELEASE_VERSION}_3.1.jar"
gpg --armor --detach-sig "flink-table-store-hive-connector-${RELEASE_VERSION}_2.3.jar"
gpg --armor --detach-sig "flink-table-store-hive-connector-${RELEASE_VERSION}_2.2.jar"
gpg --armor --detach-sig "flink-table-store-hive-connector-${RELEASE_VERSION}_2.1.jar"
gpg --armor --detach-sig "flink-table-store-spark-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "flink-table-store-spark2-${RELEASE_VERSION}.jar"
gpg --armor --detach-sig "flink-table-store-dist-${RELEASE_VERSION}_1.15.jar"
gpg --armor --detach-sig "flink-table-store-dist-${RELEASE_VERSION}_1.14.jar"

$SHASUM "flink-table-store-dist-${RELEASE_VERSION}.jar" > "flink-table-store-dist-${RELEASE_VERSION}.jar.sha512"
$SHASUM "flink-table-store-hive-catalog-${RELEASE_VERSION}_3.1.jar" > "flink-table-store-hive-catalog-${RELEASE_VERSION}_3.1.jar.sha512"
$SHASUM "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.3.jar" > "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.3.jar.sha512"
$SHASUM "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.2.jar" > "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.2.jar.sha512"
$SHASUM "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.1.jar" > "flink-table-store-hive-catalog-${RELEASE_VERSION}_2.1.jar.sha512"
$SHASUM "flink-table-store-hive-connector-${RELEASE_VERSION}_3.1.jar" > "flink-table-store-hive-connector-${RELEASE_VERSION}_3.1.jar.sha512"
$SHASUM "flink-table-store-hive-connector-${RELEASE_VERSION}_2.3.jar" > "flink-table-store-hive-connector-${RELEASE_VERSION}_2.3.jar.sha512"
$SHASUM "flink-table-store-hive-connector-${RELEASE_VERSION}_2.2.jar" > "flink-table-store-hive-connector-${RELEASE_VERSION}_2.2.jar.sha512"
$SHASUM "flink-table-store-hive-connector-${RELEASE_VERSION}_2.1.jar" > "flink-table-store-hive-connector-${RELEASE_VERSION}_2.1.jar.sha512"
$SHASUM "flink-table-store-spark-${RELEASE_VERSION}.jar" > "flink-table-store-spark-${RELEASE_VERSION}.jar.sha512"
$SHASUM "flink-table-store-spark2-${RELEASE_VERSION}.jar" > "flink-table-store-spark2-${RELEASE_VERSION}.jar.sha512"
$SHASUM "flink-table-store-dist-${RELEASE_VERSION}_1.15.jar" > "flink-table-store-dist-${RELEASE_VERSION}_1.15.jar.sha512"
$SHASUM "flink-table-store-dist-${RELEASE_VERSION}_1.14.jar" > "flink-table-store-dist-${RELEASE_VERSION}_1.14.jar.sha512"
