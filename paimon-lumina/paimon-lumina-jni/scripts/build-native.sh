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

# ==================== Auto-detect JAVA_HOME ====================
find_jdk_home() {
    # 1. Resolve from java binary symlink
    if command -v java &>/dev/null; then
        local java_bin
        java_bin="$(readlink -f "$(command -v java)")"
        local candidate
        candidate="$(dirname "$(dirname "$java_bin")")"
        if [ -d "$candidate/include" ]; then
            echo "$candidate"; return
        fi
        # java may be at jre/bin/java, so go one level up
        candidate="$(dirname "$candidate")"
        if [ -d "$candidate/include" ]; then
            echo "$candidate"; return
        fi
    fi
    # 2. Scan /usr/lib/jvm for any JDK that has include/
    for d in /usr/lib/jvm/java-*-openjdk-* /usr/lib/jvm/java /usr/lib/jvm/default-java; do
        if [ -d "$d/include" ]; then
            echo "$d"; return
        fi
    done
    return 1
}

if [ -n "$JAVA_HOME" ] && [ ! -d "$JAVA_HOME/include" ]; then
    echo "WARNING: JAVA_HOME=$JAVA_HOME has no include/ directory (JRE?), searching for a JDK..."
    unset JAVA_HOME
fi

if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME="$(find_jdk_home)" || true
fi

if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME/include" ]; then
    echo "ERROR: Could not find a JDK with JNI headers."
    echo ""
    echo "  Install the JDK development package, e.g.:"
    echo "    apt-get install openjdk-8-jdk-headless"
    echo ""
    echo "  Or set JAVA_HOME to a JDK (not a JRE) that contains include/jni.h:"
    echo "    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
    exit 1
fi
export JAVA_HOME

echo "================================================"
echo "Building Paimon Lumina JNI"
echo "================================================"
echo "LUMINA_ROOT : $LUMINA_ROOT"
echo "JAVA_HOME   : $JAVA_HOME"
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
      -DJAVA_HOME="$JAVA_HOME" \
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
