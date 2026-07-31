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

set -o errexit
set -o nounset
set -o pipefail

MVN=${MVN:-mvn}
GPG=${GPG:-gpg}
REPOSITORY_DIRECTORY=${REPOSITORY_DIRECTORY:-}
STAGING_PROFILE_ID=${STAGING_PROFILE_ID:-}
NEXUS_URL=${NEXUS_URL:-https://repository.apache.org/}
SERVER_ID=${SERVER_ID:-apache.releases.https}
NEXUS_STAGING_PLUGIN_VERSION=${NEXUS_STAGING_PLUGIN_VERSION:-1.7.0}
CUSTOM_OPTIONS=${CUSTOM_OPTIONS:-}

if [ -z "${REPOSITORY_DIRECTORY}" ]; then
    echo "REPOSITORY_DIRECTORY was not set" >&2
    exit 1
fi
if [ ! -d "${REPOSITORY_DIRECTORY}" ]; then
    echo "Maven repository does not exist: ${REPOSITORY_DIRECTORY}" >&2
    exit 1
fi
if [ -z "${STAGING_PROFILE_ID}" ]; then
    echo "STAGING_PROFILE_ID was not set" >&2
    exit 1
fi

if command -v md5sum >/dev/null 2>&1; then
    MD5_COMMAND=md5sum
else
    MD5_COMMAND=md5
fi
if command -v sha1sum >/dev/null 2>&1; then
    SHA1_COMMAND=sha1sum
else
    SHA1_COMMAND=shasum
fi

md5_value() {
    file=$1
    if [ "${MD5_COMMAND}" = "md5sum" ]; then
        md5sum "${file}" | awk '{print $1}'
    else
        md5 -q "${file}"
    fi
}

sha1_value() {
    file=$1
    if [ "${SHA1_COMMAND}" = "sha1sum" ]; then
        sha1sum "${file}" | awk '{print $1}'
    else
        shasum -a 1 "${file}" | awk '{print $1}'
    fi
}

verify_checksums() {
    file=$1
    expected_md5=$(awk 'NR == 1 {print $1}' "${file}.md5")
    expected_sha1=$(awk 'NR == 1 {print $1}' "${file}.sha1")
    actual_md5=$(md5_value "${file}")
    actual_sha1=$(sha1_value "${file}")
    if [ "${actual_md5}" != "${expected_md5}" ]; then
        echo "MD5 checksum does not match: ${file}" >&2
        exit 1
    fi
    if [ "${actual_sha1}" != "${expected_sha1}" ]; then
        echo "SHA-1 checksum does not match: ${file}" >&2
        exit 1
    fi
}

artifact_count=0
while IFS= read -r artifact; do
    if [ ! -f "${artifact}.asc" ]; then
        echo "Maven artifact is not signed: ${artifact}" >&2
        exit 1
    fi
    if [ ! -f "${artifact}.md5" ] || [ ! -f "${artifact}.sha1" ]; then
        echo "Maven artifact is missing a checksum: ${artifact}" >&2
        exit 1
    fi
    if [ ! -f "${artifact}.asc.md5" ] || [ ! -f "${artifact}.asc.sha1" ]; then
        echo "Maven signature is missing a checksum: ${artifact}.asc" >&2
        exit 1
    fi
    "${GPG}" --verify "${artifact}.asc" "${artifact}"
    verify_checksums "${artifact}"
    verify_checksums "${artifact}.asc"
    artifact_count=$((artifact_count + 1))
done < <(
    find "${REPOSITORY_DIRECTORY}" -type f \
        \( -name '*.jar' -o -name '*.pom' \) |
        LC_ALL=C sort
)

if [ "${artifact_count}" -eq 0 ]; then
    echo "Maven repository contains no JAR or POM artifacts" >&2
    exit 1
fi

echo "Verified ${artifact_count} signed Maven artifacts."
echo "Uploading the signed Maven repository image to ${NEXUS_URL}"
${MVN} -ntp \
    org.sonatype.plugins:nexus-staging-maven-plugin:${NEXUS_STAGING_PLUGIN_VERSION}:deploy-staged-repository \
    -DrepositoryDirectory="${REPOSITORY_DIRECTORY}" \
    -DnexusUrl="${NEXUS_URL}" \
    -DserverId="${SERVER_ID}" \
    -DstagingProfileId="${STAGING_PROFILE_ID}" \
    ${CUSTOM_OPTIONS} \
    -DautoReleaseAfterClose=false \
    -DkeepStagingRepositoryOnFailure=false \
    -DkeepStagingRepositoryOnCloseRuleFailure=true
