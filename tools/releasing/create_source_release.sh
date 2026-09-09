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

##
## Required variables
##
RELEASE_VERSION=${RELEASE_VERSION}

if [ -z "${RELEASE_VERSION}" ]; then
	echo "RELEASE_VERSION is unset"
	exit 1
fi

# fail immediately
set -o errexit
set -o nounset

CURR_DIR=`pwd`
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PROJECT_ROOT="$( cd "$( dirname "${BASE_DIR}/../../../" )" >/dev/null && pwd )"

# Sanity check to ensure that resolved paths are valid; a LICENSE file should aways exist in project root
if [ ! -f ${PROJECT_ROOT}/LICENSE ]; then
    echo "Project root path ${PROJECT_ROOT} is not valid; script may be in the wrong directory."
    exit 1
fi

if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

# macOS libarchive may otherwise add AppleDouble entries (._*) for filesystem
# metadata even when --no-xattrs is used. These entries are hidden by the
# macOS tar listing but are visible, and can break the build, on Linux.
export COPYFILE_DISABLE=1

###########################

RELEASE_DIR=${PROJECT_ROOT}/release
CLONE_DIR=${RELEASE_DIR}/paimon-tmp-clone

rm -rf ${RELEASE_DIR}
mkdir ${RELEASE_DIR}

# delete the temporary release directory on error
trap 'rm -rf ${RELEASE_DIR}' ERR

echo "Creating source package"

# create a temporary git clone to ensure that we have a pristine source release
git clone ${PROJECT_ROOT} ${CLONE_DIR}

cd ${CLONE_DIR}
rsync -a \
  --exclude ".git" --exclude ".gitignore" \
  --exclude ".asf.yaml" \
  --exclude "target" \
  --exclude ".idea" --exclude "*.iml" \
  --exclude ".travis.yml" \
  . paimon-${RELEASE_VERSION}

UNEXPECTED_BINARY=$(find paimon-${RELEASE_VERSION} -type f \
  \( -name "*.class" -o -name "*.jar" \) -print -quit)
if [ -n "${UNEXPECTED_BINARY}" ]; then
    echo "Source release contains a compiled binary: ${UNEXPECTED_BINARY}" >&2
    exit 1
fi

SOURCE_ARCHIVE=${RELEASE_DIR}/apache-paimon-${RELEASE_VERSION}-src.tgz
tar czf ${SOURCE_ARCHIVE} --no-xattrs paimon-${RELEASE_VERSION}

python3 - "${SOURCE_ARCHIVE}" <<'PY'
import sys
import tarfile

archive = sys.argv[1]
with tarfile.open(archive, "r:gz") as source:
    for member in source.getmembers():
        parts = [part for part in member.name.split("/") if part not in ("", ".")]
        if member.name.startswith("/") or ".." in parts:
            raise ValueError("Source release contains an unsafe path: " + member.name)
        if "__MACOSX" in parts or any(part.startswith("._") for part in parts):
            raise ValueError(
                "Source release contains macOS metadata: " + member.name
            )
PY

gpg --armor --detach-sig ${SOURCE_ARCHIVE}
cd ${RELEASE_DIR}
${SHASUM} apache-paimon-${RELEASE_VERSION}-src.tgz > apache-paimon-${RELEASE_VERSION}-src.tgz.sha512

rm -rf ${CLONE_DIR}

echo "Done. Source release package and signatures created under ${RELEASE_DIR}/."

cd ${CURR_DIR}
