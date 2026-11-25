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

for version in 1.0.1 1.4.2 2.0.0 "$current_version"; do
    mvn -q -f "$script_dir/pom.xml" "-Dpaimon.version=$version" \
        org.apache.maven.plugins:maven-dependency-plugin:3.7.1:build-classpath \
        "-Dmdep.outputFile=$test_dir/classpath-$version"
    mkdir -p "$test_dir/classes-$version"
    javac -source 8 -target 8 \
        -cp "$(cat "$test_dir/classpath-$version")" \
        -d "$test_dir/classes-$version" \
        "$script_dir/src/main/java/org/apache/paimon/compatibility/ReleaseTableCompatibility.java"
done

run() {
    local version=$1 warehouse=$2 stage=$3
    java -cp "$test_dir/classes-$version:$(cat "$test_dir/classpath-$version")" \
        org.apache.paimon.compatibility.ReleaseTableCompatibility "$warehouse" "$stage"
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
    if run 1.0.1 "$warehouse" readBlob > "$test_dir/blob-$newer.log" 2>&1; then
        echo "Paimon 1.0.1 unexpectedly opened a $newer BLOB table" >&2
        exit 1
    fi
    if ! grep -q 'Input type string: BLOB' "$test_dir/blob-$newer.log"; then
        cat "$test_dir/blob-$newer.log" >&2
        exit 1
    fi
done
