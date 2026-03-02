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
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################
# Build libpaimon_lumina_jni.so — single fat .so with all dependencies.
#
# Usage:
#   LUMINA_ROOT=/path/to/lumina ./build-native.sh
#   LUMINA_ROOT=/path/to/paimon-cpp/third_party/lumina ./build-native.sh
#
# Prerequisites:
#   - CMake >= 3.14, make, patchelf
#   - C++17 compiler (GCC >= 9.3)
#   - JDK (JAVA_HOME)
#   - Lumina library (LUMINA_ROOT with include/ and lib/)
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="$PROJECT_DIR/src/main/native"
BUILD_DIR="$NATIVE_DIR/build"

# ==================== Validate ====================
if [ -z "$LUMINA_ROOT" ]; then
    echo "ERROR: LUMINA_ROOT is not set."
    echo "  export LUMINA_ROOT=/path/to/paimon-cpp/third_party/lumina"
    exit 1
fi
if [ ! -d "$LUMINA_ROOT/include/lumina" ]; then
    echo "ERROR: Lumina headers not found at $LUMINA_ROOT/include/lumina"
    exit 1
fi

echo "================================================"
echo "Building Paimon Lumina JNI"
echo "================================================"
echo "LUMINA_ROOT : $LUMINA_ROOT"
echo "JAVA_HOME   : ${JAVA_HOME:-<auto-detect>}"
echo ""

# ==================== Detect platform ====================
OS=$(uname -s)
ARCH=$(uname -m)

if [ "$OS" = "Linux" ]; then
    PLATFORM_OS="linux"
elif [ "$OS" = "Darwin" ]; then
    PLATFORM_OS="darwin"
else
    echo "Unsupported OS: $OS"; exit 1
fi

case "$ARCH" in
    x86_64|amd64)  PLATFORM_ARCH="amd64" ;;
    aarch64|arm64) PLATFORM_ARCH="aarch64" ;;
    *)             echo "Unsupported arch: $ARCH"; exit 1 ;;
esac

OUTPUT_DIR="$PROJECT_DIR/src/main/resources/$PLATFORM_OS/$PLATFORM_ARCH"
mkdir -p "$OUTPUT_DIR"
echo "Platform    : $PLATFORM_OS/$PLATFORM_ARCH"
echo "Output      : $OUTPUT_DIR"
echo ""

# ==================== Build ====================
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

cmake -DCMAKE_BUILD_TYPE=Release \
      -DLUMINA_ROOT="$LUMINA_ROOT" \
      ..

make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

# ==================== Install ====================
# Move the built .so to the JAR resource directory
SO_NAME="libpaimon_lumina_jni.so"
if [ "$OS" = "Darwin" ]; then
    SO_NAME="libpaimon_lumina_jni.dylib"
fi

cp "$BUILD_DIR/$SO_NAME" "$OUTPUT_DIR/$SO_NAME"

# ==================== patchelf ====================
if [ "$OS" = "Linux" ] && command -v patchelf &>/dev/null; then
    echo ""
    echo "Setting rpath..."
    patchelf --force-rpath --set-rpath \
        '/usr/local/lib64:/usr/lib/jvm/java/jre/lib/amd64:/usr/lib/jvm/java/jre/lib/amd64/server' \
        "$OUTPUT_DIR/$SO_NAME"
    echo "Done"
fi

# ==================== Summary ====================
echo ""
echo "============================================"
echo "Build completed successfully!"
echo "============================================"
echo ""
echo "Library: $OUTPUT_DIR/$SO_NAME"
ls -lh "$OUTPUT_DIR/$SO_NAME"
echo ""

if [ "$OS" = "Linux" ]; then
    echo "Dependencies:"
    ldd "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true
elif [ "$OS" = "Darwin" ]; then
    echo "Dependencies:"
    otool -L "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true
fi

echo ""
echo "To package into JAR:"
echo "  mvn -pl paimon-lumina/paimon-lumina-jni package"
