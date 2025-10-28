#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -e
wget https://security.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1-1ubuntu2.1~18.04.23_amd64.deb
sudo dpkg -i libssl1.1_1.1.1-1ubuntu2.1~18.04.23_amd64.deb

function random_timezone() {
    local rnd=$(expr $RANDOM % 25)
    local hh=$(expr $rnd / 2)
    local mm=$(expr $rnd % 2 \* 3)"0"
    local sgn=$(expr $RANDOM % 2)
    if [ $sgn -eq 0 ]
    then
        echo "GMT+$hh:$mm"
    else
        echo "GMT-$hh:$mm"
    fi
}

function run_flink_e2e_tests() {
    jvm_timezone=$(random_timezone)
    echo "JVM timezone is set to $jvm_timezone"
    profile="flink-$flink_version"
    mvn -T 1C -B test -Pflink1,spark3 -pl paimon-e2e-tests -Duser.timezone=$jvm_timezone -P${profile}
}