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
# Build libpaimon_lumina_jni.so with liblumina.so bundled.
#
# Usage:
#   LUMINA_ROOT=/path/to/lumina ./build-native.sh
#
# Expected layout under LUMINA_ROOT:
#   include/lumina/api/LuminaBuilder.h  (headers)
#   lib/liblumina.so                    (shared library)
#
# Output: libpaimon_lumina_jni.so + liblumina.so bundled together,
#         with libstdc++/libgcc statically linked into the JNI library.
#
# Prerequisites:
#   - CMake >= 3.14, make, patchelf
#   - C++17 compiler (GCC >= 9.3)
#   - JDK (JAVA_HOME)
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="$PROJECT_DIR/src/main/native"
BUILD_DIR="$NATIVE_DIR/build"

# ==================== Validate LUMINA_ROOT ====================
if [ -z "$LUMINA_ROOT" ]; then
    echo "ERROR: LUMINA_ROOT is not set."
    echo "  export LUMINA_ROOT=/path/to/lumina"
    exit 1
fi
if [ ! -d "$LUMINA_ROOT/include/lumina" ]; then
    echo "ERROR: Lumina headers not found at $LUMINA_ROOT/include/lumina"
    exit 1
fi
if [ ! -f "$LUMINA_ROOT/lib/liblumina.so" ]; then
    echo "ERROR: liblumina.so not found at $LUMINA_ROOT/lib/liblumina.so"
    exit 1
fi

# ==================== Auto-detect JAVA_HOME ====================
find_jdk_home() {
    if command -v java &>/dev/null; then
        local java_bin
        java_bin="$(readlink -f "$(command -v java)")"
        local candidate
        candidate="$(dirname "$(dirname "$java_bin")")"
        if [ -d "$candidate/include" ]; then
            echo "$candidate"; return
        fi
        candidate="$(dirname "$candidate")"
        if [ -d "$candidate/include" ]; then
            echo "$candidate"; return
        fi
    fi
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
    echo "  Install: apt-get install openjdk-8-jdk-headless"
    echo "  Or set:  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
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
echo "Platform : $PLATFORM_OS/$PLATFORM_ARCH"
echo "Output   : $OUTPUT_DIR"
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
JNI_SO="libpaimon_lumina_jni.so"
LUMINA_SO="liblumina.so"
if [ "$OS" = "Darwin" ]; then
    JNI_SO="libpaimon_lumina_jni.dylib"
fi

cp "$BUILD_DIR/$JNI_SO" "$OUTPUT_DIR/$JNI_SO"
cp "$LUMINA_ROOT/lib/$LUMINA_SO" "$OUTPUT_DIR/$LUMINA_SO"

# ==================== patchelf ====================
if [ "$OS" = "Linux" ] && command -v patchelf &>/dev/null; then
    echo ""
    echo "Setting rpath with \$ORIGIN..."
    patchelf --force-rpath --set-rpath '$ORIGIN' "$OUTPUT_DIR/$JNI_SO"
    patchelf --force-rpath --set-rpath '$ORIGIN' "$OUTPUT_DIR/$LUMINA_SO"
    echo "Done"
fi

# ==================== Verification ====================
echo ""
echo "============================================"
echo "Build completed!"
echo "============================================"
echo ""
echo "Output files:"
ls -lh "$OUTPUT_DIR/$JNI_SO"
ls -lh "$OUTPUT_DIR/$LUMINA_SO"
echo ""

if [ "$OS" = "Linux" ]; then
    echo "Dynamic dependencies of $JNI_SO:"
    ldd "$OUTPUT_DIR/$JNI_SO" 2>/dev/null || true
    echo ""
fi

echo "To package into JAR:"
echo "  mvn -pl paimon-lumina/paimon-lumina-jni package"
