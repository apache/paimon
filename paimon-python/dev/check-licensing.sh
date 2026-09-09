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

download_rat_jar () {
    URL="https://repo.maven.apache.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"
    JAR="$rat_jar"

    # Download rat launch jar
    echo "Attempting to fetch rat"
    wget --quiet ${URL} -O "$JAR"
}

SOURCE_PACKAGE=${SOURCE_PACKAGE:-}

export RAT_VERSION=0.15
export rat_jar=rat/apache-rat-${RAT_VERSION}.jar
PACKAGE_DIR=""
TEMP_DIR=""

cleanup () {
    rm -rf rat
    if [ -n "${TEMP_DIR}" ] && [ -d "${TEMP_DIR}" ]; then
        rm -rf "${TEMP_DIR}"
    fi
}

trap cleanup EXIT

if [ -z "${SOURCE_PACKAGE}" ]; then
    BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
    PROJECT_ROOT="${BASE_DIR}/../"
    cd ${PROJECT_ROOT}

    # Sanity check to ensure that resolved paths are valid; a LICENSE file should always exist in project root
    if [ ! -f ${PROJECT_ROOT}/setup.py ]; then
        echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
        exit 1
    fi

    RUN_RAT=(
        java -jar "${rat_jar}"
        -E "${PROJECT_ROOT}/dev/.rat-excludes"
        -d "${PROJECT_ROOT}"
    )
else
    EXTENSION='.tar.gz'
    # get unzipped directory
    PACKAGE_NAME="$(basename "${SOURCE_PACKAGE}" "${EXTENSION}")"
    TEMP_DIR="$(mktemp -d /tmp/pypaimon-rat.XXXXXX)"
    PACKAGE_DIR="${TEMP_DIR}/${PACKAGE_NAME}"
    tar -xf "${SOURCE_PACKAGE}" -C "${TEMP_DIR}"
    if [ ! -d "${PACKAGE_DIR}" ]; then
        echo "Source package did not contain ${PACKAGE_NAME}" >&2
        exit 1
    fi

    RUN_RAT=(
        java -jar "${rat_jar}"
        -e '.*PKG-INFO$'
        -e '.*setup[.]cfg$'
        -e '.*SOURCES[.]txt$'
        -e '.*dependency_links[.]txt$'
        -e '.*entry_points[.]txt$'
        -e '.*requires[.]txt$'
        -e '.*top_level[.]txt$'
        -e '.*_full_version$'
        -e '.*[.]jsonl$'
        -d "${PACKAGE_DIR}"
    )
fi

mkdir -p rat
download_rat_jar

if ! "${RUN_RAT[@]}" > rat/rat-results.txt; then
    echo "RAT exited abnormally"
    exit 1
fi

ERRORS="$(grep -e "??" rat/rat-results.txt)"

if [[ -n "${ERRORS}" ]]; then
    echo "Could not find Apache license headers in the following files:"
    echo ${ERRORS}
    exit 1
else
    echo -e "RAT checks passed."
fi
