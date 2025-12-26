#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script to build the FAISS JNI native library

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# Detect platform
OS_NAME=$(uname -s)
ARCH=$(uname -m)

case "$OS_NAME" in
    Linux*)
        PLATFORM="linux"
        ;;
    Darwin*)
        PLATFORM="darwin"
        ;;
    *)
        echo "Unsupported OS: $OS_NAME"
        exit 1
        ;;
esac

case "$ARCH" in
    x86_64|amd64)
        ARCH_NAME="x86_64"
        ;;
    aarch64|arm64)
        ARCH_NAME="aarch64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

NATIVE_OUTPUT_DIR="${SCRIPT_DIR}/../resources/native/${PLATFORM}-${ARCH_NAME}"

echo "Building FAISS JNI for ${PLATFORM}-${ARCH_NAME}..."

# Check dependencies on macOS
if [ "$PLATFORM" = "darwin" ]; then
    echo "Checking macOS dependencies..."
    
    # Check for libomp
    if ! brew list libomp &>/dev/null; then
        echo "Installing libomp (required for OpenMP support)..."
        brew install libomp
    fi
    
    # Check for faiss
    if ! brew list faiss &>/dev/null; then
        echo "Installing faiss..."
        brew install faiss
    fi
fi

# Check dependencies on Linux
if [ "$PLATFORM" = "linux" ]; then
    echo "Checking Linux dependencies..."
    
    # Check GCC version
    GCC_VERSION=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
    if [ -n "$GCC_VERSION" ] && [ "$GCC_VERSION" -lt 5 ]; then
        echo "WARNING: GCC version $GCC_VERSION is too old. C++14 or later requires GCC 5+."
        echo "Please install a newer GCC:"
        echo "  On Ubuntu/Debian: sudo apt-get install g++-7 (or newer)"
        echo "  On CentOS/RHEL: sudo yum install devtoolset-7-gcc-c++ && scl enable devtoolset-7 bash"
    fi
    
    # Check for FAISS
    if [ ! -f /usr/local/lib/libfaiss.so ] && [ ! -f /usr/lib/libfaiss.so ] && [ ! -f /usr/lib64/libfaiss.so ]; then
        echo "FAISS library not found in standard paths."
        echo "Please install FAISS:"
        echo "  Option 1: conda install -c pytorch faiss-cpu"
        echo "  Option 2: Build from source: https://github.com/facebookresearch/faiss"
        echo ""
        echo "If FAISS is installed in a custom location, set FAISS_HOME environment variable."
    fi
fi

# Clean and create build directory
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Configure with CMake
cmake -DCMAKE_BUILD_TYPE=Release ..

# Build
cmake --build . --config Release

# Create output directory
mkdir -p "${NATIVE_OUTPUT_DIR}"

# Copy the library
if [ "$PLATFORM" = "darwin" ]; then
    cp "${BUILD_DIR}/lib/libpaimon_faiss_jni.dylib" "${NATIVE_OUTPUT_DIR}/"
else
    cp "${BUILD_DIR}/lib/libpaimon_faiss_jni.so" "${NATIVE_OUTPUT_DIR}/"
fi

echo "Build complete. Native library copied to: ${NATIVE_OUTPUT_DIR}"

