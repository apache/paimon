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

#!/usr/bin/env bash

set -euo pipefail

mode="${1:-java}"
workspace="${GITHUB_WORKSPACE:-$(pwd)}"
paimon_full_text_parent="${PAIMON_FULL_TEXT_PARENT_DIR:-${RUNNER_TEMP:-${workspace}/..}}"
paimon_full_text_dir="${PAIMON_FULL_TEXT_DIR:-${paimon_full_text_parent}/paimon-full-text}"
paimon_full_text_ref="${PAIMON_FULL_TEXT_REF:-main}"
paimon_full_text_repository="${PAIMON_FULL_TEXT_REPOSITORY:-https://github.com/apache/paimon-full-text.git}"

if [[ "${mode}" != "java" && "${mode}" != "native" ]]; then
  echo "Usage: $0 [java|native]" >&2
  exit 1
fi

echo "Preparing paimon-full-text from ${paimon_full_text_repository}@${paimon_full_text_ref}"
mkdir -p "$(dirname "${paimon_full_text_dir}")"

if [[ -e "${paimon_full_text_dir}" && ! -d "${paimon_full_text_dir}/.git" ]]; then
  rm -rf "${paimon_full_text_dir}"
fi

if [[ ! -d "${paimon_full_text_dir}/.git" ]]; then
  git init "${paimon_full_text_dir}"
  git -C "${paimon_full_text_dir}" remote add origin "${paimon_full_text_repository}"
else
  git -C "${paimon_full_text_dir}" remote set-url origin "${paimon_full_text_repository}"
fi

git -C "${paimon_full_text_dir}" fetch --depth=1 origin "${paimon_full_text_ref}"
git -C "${paimon_full_text_dir}" checkout --force FETCH_HEAD

echo "PAIMON_FULL_TEXT_DIR=${paimon_full_text_dir}"
if [[ -n "${GITHUB_ENV:-}" ]]; then
  echo "PAIMON_FULL_TEXT_DIR=${paimon_full_text_dir}" >> "${GITHUB_ENV}"
fi

if [[ "${mode}" == "native" ]]; then
  cargo build --manifest-path "${paimon_full_text_dir}/Cargo.toml" --release \
    -p paimon-ftindex-jni \
    -p paimon-ftindex-ffi

  case "$(uname -s)" in
    Darwin)
      lib_extension="dylib"
      ;;
    *)
      lib_extension="so"
      ;;
  esac

  jni_lib="${paimon_full_text_dir}/target/release/libpaimon_ftindex_jni.${lib_extension}"
  ffi_lib="${paimon_full_text_dir}/target/release/libpaimon_ftindex_ffi.${lib_extension}"

  echo "PAIMON_FTINDEX_JNI_LIB_PATH=${jni_lib}"
  echo "PAIMON_FTINDEX_LIB_PATH=${ffi_lib}"
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "PAIMON_FTINDEX_JNI_LIB_PATH=${jni_lib}" >> "${GITHUB_ENV}"
    echo "PAIMON_FTINDEX_LIB_PATH=${ffi_lib}" >> "${GITHUB_ENV}"
  fi
fi

mvn -B -ntp -f "${paimon_full_text_dir}/java/pom.xml" install -DskipTests
