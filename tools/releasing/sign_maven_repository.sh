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

REPOSITORY_DIRECTORY=${REPOSITORY_DIRECTORY:-}
GPG=${GPG:-gpg}
GPG_KEY_ID=${GPG_KEY_ID:-}

if [ -z "${REPOSITORY_DIRECTORY}" ]; then
    echo "REPOSITORY_DIRECTORY was not set" >&2
    exit 1
fi
if [ ! -d "${REPOSITORY_DIRECTORY}" ]; then
    echo "Maven repository does not exist: ${REPOSITORY_DIRECTORY}" >&2
    exit 1
fi
if find "${REPOSITORY_DIRECTORY}" -type f -name '*.asc' -print -quit |
    grep -q .; then
    echo "Maven repository already contains signatures" >&2
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

write_md5() {
    file=$1
    if [ "${MD5_COMMAND}" = "md5sum" ]; then
        md5sum "${file}" | awk '{print $1}' > "${file}.md5"
    else
        md5 -q "${file}" > "${file}.md5"
    fi
}

write_sha1() {
    file=$1
    if [ "${SHA1_COMMAND}" = "sha1sum" ]; then
        sha1sum "${file}" | awk '{print $1}' > "${file}.sha1"
    else
        shasum -a 1 "${file}" | awk '{print $1}' > "${file}.sha1"
    fi
}

artifact_count=0
while IFS= read -r artifact; do
    gpg_arguments=(--armor --detach-sign)
    if [ -n "${GPG_KEY_ID}" ]; then
        gpg_arguments+=(--local-user "${GPG_KEY_ID}")
    fi

    "${GPG}" "${gpg_arguments[@]}" \
        --output "${artifact}.asc" "${artifact}"
    "${GPG}" --verify "${artifact}.asc" "${artifact}"
    write_md5 "${artifact}.asc"
    write_sha1 "${artifact}.asc"
    artifact_count=$((artifact_count + 1))
done < <(
    find "${REPOSITORY_DIRECTORY}" -type f \
        \( -name '*.jar' -o -name '*.pom' \) |
        LC_ALL=C sort
)

if [ "${artifact_count}" -eq 0 ]; then
    echo "No Maven artifacts found to sign" >&2
    exit 1
fi

echo "Signed and verified ${artifact_count} Maven artifacts."
