#!/usr/bin/env bash

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

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
current_version=$(mvn -q -f "$repo_root/pom.xml" help:evaluate \
    -Dexpression=project.version -DforceStdout)
current_jar="$repo_root/paimon-bundle/target/paimon-bundle-$current_version.jar"
if [[ ! -s "$current_jar" ]]; then
    echo "Build $current_jar before running the compatibility test" >&2
    exit 1
fi
echo "Using current checkout bundle: $current_jar"

for version in 1.0.1 1.4.2 2.0.0; do
    mvn -q -f "$script_dir/pom.xml" -Preleased-bundle "-Dpaimon.version=$version" \
        org.apache.maven.plugins:maven-dependency-plugin:3.7.1:build-classpath \
        -Dmdep.includeScope=test \
        "-Dmdep.outputFile=$test_dir/classpath-$version"
done

mvn -q -f "$script_dir/pom.xml" \
    org.apache.maven.plugins:maven-dependency-plugin:3.7.1:build-classpath \
    -Dmdep.includeScope=test \
    "-Dmdep.outputFile=$test_dir/classpath-current"
if [[ "$(cat "$test_dir/classpath-current")" == *'/org/apache/paimon/'* ]]; then
    echo "Current build classpath unexpectedly contains a resolved Paimon artifact" >&2
    exit 1
fi
printf '%s:%s\n' "$current_jar" "$(cat "$test_dir/classpath-current")" \
    > "$test_dir/classpath-$current_version"

for version in 1.0.1 1.4.2 2.0.0 "$current_version"; do
    mkdir -p "$test_dir/classes-$version"
    javac -source 8 -target 8 \
        -cp "$(cat "$test_dir/classpath-$version")" \
        -d "$test_dir/classes-$version" \
        "$script_dir/src/test/java/org/apache/paimon/compatibility/ReleaseTableCompatibilityTest.java"
done

run() {
    local version=$1 warehouse=$2 stage=$3
    echo "Testing $stage with Paimon $version"
    java "-Dcompat.warehouse=$warehouse" "-Dcompat.stage=$stage" \
        -cp "$test_dir/classes-$version:$(cat "$test_dir/classpath-$version")" \
        org.junit.platform.console.ConsoleLauncher execute \
        --select-class org.apache.paimon.compatibility.ReleaseTableCompatibilityTest \
        --disable-banner --disable-ansi-colors --details=none
}

for newer in 1.4.2 2.0.0 "$current_version"; do
    warehouse="$test_dir/table-$newer"
    run 1.0.1 "$warehouse" create
    run "$newer" "$warehouse" read1
    run "$newer" "$warehouse" write2
    run 1.0.1 "$warehouse" read2
    run 1.0.1 "$warehouse" write3
    run "$newer" "$warehouse" read3

    # A newer type is not covered by the basic read/write compatibility result.
    run "$newer" "$warehouse" createBlob
    run 1.0.1 "$warehouse" readBlob
done
