#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

suite=${1:?Specify a CI suite}
scala=${2:-2.12}
java=${3:-8}
profiles=flink1,spark3
threads=2C
goal=verify
test_options=(-Dstyle.color=never)
modules=()

case "$suite" in
    core)
        # Keep spark-common and paimon-docs here: they have their own tests,
        # including JDK 11 coverage, outside the Spark connector test modules.
        modules=('!paimon-e2e-tests')
        for version in ut 3.5 3.4 3.3 3.2; do
            modules+=("!org.apache.paimon:paimon-spark-${version}_2.12")
        done
        if [[ "$java" == 11 ]]; then
            modules+=('!org.apache.paimon:paimon-hive-connector-3.1')
            threads=1C
        fi
        test_options+=(-Pskip-paimon-flink-tests)
        ;;
    flink1-common)
        modules=(paimon-flink/paimon-flink-common)
        ;;
    flink1-connectors)
        for version in cdc 1.16 1.17 1.18 1.19 1.20; do
            modules+=("org.apache.paimon:paimon-flink-${version}")
        done
        ;;
    flink2)
        profiles=flink2,spark3
        for version in 2.0 2.1 2.2 common; do
            modules+=("org.apache.paimon:paimon-flink-${version}")
        done
        ;;
    spark3)
        profiles="flink1,spark3,scala-${scala}"
        for version in ut 3.5 3.4 3.3 3.2; do
            modules+=("org.apache.paimon:paimon-spark-${version}_${scala}")
        done
        ;;
    spark4)
        profiles=flink1,spark4
        for version in ut 4.0 4.1; do
            modules+=("org.apache.paimon:paimon-spark-${version}_2.13")
        done
        ;;
    e2e-flink1)
        profiles=flink1,spark3,flink-1.20
        modules=(paimon-e2e-tests)
        threads=1C
        goal=test
        ;;
    e2e-flink2)
        profiles=flink2,spark3,flink-2.2,java11
        modules=(paimon-e2e-tests)
        threads=1C
        goal=test
        ;;
    iceberg)
        profiles=flink1,spark3,paimon-iceberg,iceberg-ga
        modules=(paimon-iceberg)
        threads=1C
        goal=test
        ;;
    eslib)
        profiles=paimon-eslib
        modules=(paimon-eslib)
        threads=1
        ;;
    full-text)
        modules=(paimon-full-text)
        threads=1
        ;;
    *)
        echo "Unknown CI suite: $suite" >&2
        exit 1
        ;;
esac

module_list=$(IFS=,; echo "${modules[*]}")

run_maven() {
    if [[ "${CI_DRY_RUN:-false}" == true ]]; then
        printf '%q ' mvn "$@"
        printf '\n'
    else
        mvn "$@"
    fi
}

# -am compiles and installs the selected tests' dependency closure, including
# test jars. Only the selected modules run tests in the second invocation.
run_maven -T 2C -B -ntp install -DskipTests -P"$profiles" -pl "$module_list" -am

jvm_timezone=GMT+00:00
if [[ "${CI_DRY_RUN:-false}" != true ]]; then
    # Preserve the native-library setup used by the original engine/core lanes.
    if [[ "$suite" != eslib && "$suite" != full-text ]]; then
        . tools/ci/utils.sh
        jvm_timezone=$(random_timezone)
    fi
    echo "JVM timezone is set to $jvm_timezone"
fi

# verify already includes the test phase; don't execute `test verify` or clean
# away the dependencies and test classes just built in this fresh checkout.
run_maven -T "$threads" -B -ntp "$goal" -P"$profiles" -pl "$module_list" \
    "${test_options[@]}" -Duser.timezone="$jvm_timezone"
