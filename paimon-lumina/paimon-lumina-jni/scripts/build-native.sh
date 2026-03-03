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
# Build libpaimon_lumina_jni.so — single fat .so with Lumina statically linked.
#
# Usage:
#   LUMINA_ROOT=/path/to/lumina ./build-native.sh
#   LUMINA_ROOT=/path/to/lumina LUMINA_LIBRARY=/path/to/liblumina.a ./build-native.sh
#
# The script auto-searches for liblumina.a under LUMINA_ROOT.
# If your .a is elsewhere, set LUMINA_LIBRARY explicitly.
#
# Prerequisites:
#   - CMake >= 3.14, make, patchelf
#   - C++17 compiler (GCC >= 9.3)
#   - JDK (JAVA_HOME)
#   - Lumina static library (liblumina.a)
################################################################################

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="$PROJECT_DIR/src/main/native"
BUILD_DIR="$NATIVE_DIR/build"

# ==================== Validate LUMINA_ROOT ====================
if [ -z "$LUMINA_ROOT" ]; then
    echo "ERROR: LUMINA_ROOT is not set."
    echo "  export LUMINA_ROOT=/path/to/paimon-cpp/third_party/lumina"
    exit 1
fi
if [ ! -d "$LUMINA_ROOT/include/lumina" ]; then
    echo "ERROR: Lumina headers not found at $LUMINA_ROOT/include/lumina"
    exit 1
fi

# ==================== Find liblumina.a ====================
if [ -z "$LUMINA_LIBRARY" ]; then
    echo "Searching for liblumina.a ..."
    # Search common locations under LUMINA_ROOT
    for candidate in \
        "$LUMINA_ROOT/lib/liblumina.a" \
        "$LUMINA_ROOT/lib64/liblumina.a" \
        "$LUMINA_ROOT/build/lib/liblumina.a" \
        "$LUMINA_ROOT/build/liblumina.a"; do
        if [ -f "$candidate" ]; then
            LUMINA_LIBRARY="$candidate"
            break
        fi
    done

    # Recursive search as last resort
    if [ -z "$LUMINA_LIBRARY" ]; then
        LUMINA_LIBRARY="$(find "$LUMINA_ROOT" -name 'liblumina.a' -type f 2>/dev/null | head -1)" || true
    fi

    if [ -z "$LUMINA_LIBRARY" ] || [ ! -f "$LUMINA_LIBRARY" ]; then
        echo ""
        echo "ERROR: liblumina.a not found under LUMINA_ROOT=$LUMINA_ROOT"
        echo ""
        echo "  A fat .so requires the static library liblumina.a."
        echo "  Files found under $LUMINA_ROOT/lib/:"
        ls -la "$LUMINA_ROOT/lib/" 2>/dev/null || echo "    (directory does not exist)"
        echo ""
        echo "  If liblumina.a is elsewhere, set it explicitly:"
        echo "    LUMINA_LIBRARY=/path/to/liblumina.a ./build-native.sh"
        echo ""
        echo "  To build liblumina.a from source, rebuild Lumina with:"
        echo "    cmake -DBUILD_SHARED_LIBS=OFF ... && make"
        exit 1
    fi
fi

if [ ! -f "$LUMINA_LIBRARY" ]; then
    echo "ERROR: LUMINA_LIBRARY=$LUMINA_LIBRARY does not exist."
    exit 1
fi

echo "Found Lumina static library: $LUMINA_LIBRARY"
ls -lh "$LUMINA_LIBRARY"
echo ""

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
echo "Building Paimon Lumina JNI (fat .so)"
echo "================================================"
echo "LUMINA_ROOT    : $LUMINA_ROOT"
echo "LUMINA_LIBRARY : $LUMINA_LIBRARY"
echo "JAVA_HOME      : $JAVA_HOME"
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
      -DLUMINA_LIBRARY="$LUMINA_LIBRARY" \
      -DJAVA_HOME="$JAVA_HOME" \
      ..

make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

# ==================== Install ====================
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
        '$ORIGIN:/usr/local/lib64:/usr/lib/jvm/java/jre/lib/amd64:/usr/lib/jvm/java/jre/lib/amd64/server' \
        "$OUTPUT_DIR/$SO_NAME"
    echo "Done"
fi

# ==================== Verification ====================
echo ""
echo "============================================"
echo "Build completed!"
echo "============================================"
echo ""

echo "Output:"
ls -lh "$OUTPUT_DIR/$SO_NAME"
echo ""

LIB_SIZE_KB=$(du -k "$OUTPUT_DIR/$SO_NAME" | cut -f1)
if [ "$LIB_SIZE_KB" -lt 1024 ]; then
    echo "WARNING: Library is only ${LIB_SIZE_KB}K — Lumina may not be embedded!"
    echo "  Expected a fat .so of several MB."
    echo "  Check the CMake/linker output above for errors."
else
    echo "OK: Fat .so is ${LIB_SIZE_KB}K (Lumina + JNI + libstdc++ statically linked)"
fi
echo ""

if [ "$OS" = "Linux" ]; then
    echo "Dynamic dependencies (should NOT include liblumina):"
    ldd "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true

    echo ""
    if ldd "$OUTPUT_DIR/$SO_NAME" 2>/dev/null | grep -q "liblumina"; then
        echo "WARNING: liblumina.so still appears as a dynamic dependency!"
        echo "  The fat .so may not be self-contained."
    else
        echo "OK: No dynamic dependency on liblumina — fully self-contained."
    fi
elif [ "$OS" = "Darwin" ]; then
    echo "Dependencies:"
    otool -L "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true
fi

echo ""
echo "To package into JAR:"
echo "  mvn -pl paimon-lumina/paimon-lumina-jni package"
