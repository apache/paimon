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
# Build libpaimon_lumina_jni.so and bundle liblumina.so alongside it.
#
# Usage:
#   LUMINA_ROOT=/path/to/lumina ./build-native.sh
#
# Expected layout under LUMINA_ROOT:
#   include/lumina/api/LuminaBuilder.h  (headers)
#   lib/liblumina.so                    (shared library)
#
# Output directory will contain:
#   libpaimon_lumina_jni.so   (JNI library, RPATH=$ORIGIN)
#   liblumina.so              (copied from LUMINA_ROOT, RPATH=$ORIGIN)
#
# At runtime the two .so files must reside in the same directory.
# The NativeLibraryLoader extracts them from the JAR into a temp dir;
# the OS dynamic linker resolves liblumina.so via RPATH=$ORIGIN.
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

# ==================== Auto-detect C++17 compiler (devtoolset) ====================
if [ -z "$CXX" ]; then
    for ver in 13 12 11 10 9; do
        devtoolset="/opt/rh/devtoolset-${ver}/enable"
        if [ -f "$devtoolset" ]; then
            echo "Activating devtoolset-${ver} for C++17 support..."
            source "$devtoolset"
            break
        fi
    done
fi

# Verify the compiler supports C++17
CXX_CMD="${CXX:-g++}"
if ! echo '#include <variant>' | "$CXX_CMD" -std=c++17 -x c++ -E - &>/dev/null; then
    echo "ERROR: $CXX_CMD does not support C++17 (<variant> header not found)."
    echo "  Install a newer GCC: yum install devtoolset-9-gcc-c++"
    echo "  Or set CXX to a C++17-capable compiler."
    exit 1
fi
echo "C++ compiler : $CXX_CMD ($(${CXX_CMD} -dumpversion))"

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

CMAKE_EXTRA_ARGS=""
if [ -n "$CC" ]; then
    CMAKE_EXTRA_ARGS="$CMAKE_EXTRA_ARGS -DCMAKE_C_COMPILER=$CC"
fi
if [ -n "$CXX" ]; then
    CMAKE_EXTRA_ARGS="$CMAKE_EXTRA_ARGS -DCMAKE_CXX_COMPILER=$CXX"
fi

cmake -DCMAKE_BUILD_TYPE=Release \
      -DLUMINA_ROOT="$LUMINA_ROOT" \
      -DJAVA_HOME="$JAVA_HOME" \
      $CMAKE_EXTRA_ARGS \
      ..

make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

# ==================== Install: copy both .so files ====================
JNI_SO="libpaimon_lumina_jni.so"
LUMINA_SO="liblumina.so"
if [ "$OS" = "Darwin" ]; then
    JNI_SO="libpaimon_lumina_jni.dylib"
    LUMINA_SO="liblumina.dylib"
fi

cp -L "$BUILD_DIR/$JNI_SO"           "$OUTPUT_DIR/$JNI_SO"
cp -L "$LUMINA_ROOT/lib/$LUMINA_SO"  "$OUTPUT_DIR/$LUMINA_SO"

# ==================== Bundle libstdc++.so.6 for liblumina.so ====================
# liblumina.so dynamically links libstdc++, but the system's version (GCC 4.8)
# may be too old.  Bundle a newer version so RPATH=$ORIGIN picks it up.
if [ "$OS" = "Linux" ]; then
    STDCXX_SO=""

    # Method 1: ask the compiler
    for name in libstdc++.so.6 libstdc++.so; do
        candidate="$("$CXX_CMD" -print-file-name="$name" 2>/dev/null)"
        if [ -n "$candidate" ] && [ "$candidate" != "$name" ] && [ -f "$candidate" ]; then
            STDCXX_SO="$candidate"; break
        fi
    done

    # Method 2: search compiler's lib directory
    if [ -z "$STDCXX_SO" ]; then
        CXX_LIBDIR="$(dirname "$("$CXX_CMD" -print-file-name=libstdc++.a 2>/dev/null)" 2>/dev/null)"
        if [ -d "$CXX_LIBDIR" ]; then
            for f in "$CXX_LIBDIR"/libstdc++.so.6*; do
                [ -f "$f" ] && STDCXX_SO="$f" && break
            done
        fi
    fi

    # Method 3: search devtoolset paths
    if [ -z "$STDCXX_SO" ]; then
        for f in /opt/rh/devtoolset-*/root/usr/lib64/libstdc++.so.6* \
                 /opt/rh/devtoolset-*/root/usr/lib/gcc/x86_64-*/*/libstdc++.so*; do
            [ -f "$f" ] && STDCXX_SO="$f" && break
        done
    fi

    # Method 4: search LD_LIBRARY_PATH
    if [ -z "$STDCXX_SO" ] && [ -n "$LD_LIBRARY_PATH" ]; then
        IFS=':' read -ra _DIRS <<< "$LD_LIBRARY_PATH"
        for dir in "${_DIRS[@]}"; do
            for f in "$dir"/libstdc++.so.6*; do
                [ -f "$f" ] && STDCXX_SO="$f" && break 2
            done
        done
    fi

    if [ -n "$STDCXX_SO" ] && [ -f "$STDCXX_SO" ]; then
        echo ""
        echo "Bundling libstdc++.so.6 from: $STDCXX_SO"
        cp -L "$STDCXX_SO" "$OUTPUT_DIR/libstdc++.so.6"
    else
        echo ""
        echo "WARNING: Could not locate a newer libstdc++.so.6"
        echo "  liblumina.so may fail on systems with old libstdc++"
        echo "  You can manually copy it: cp /path/to/libstdc++.so.6 $OUTPUT_DIR/"
    fi
fi

# ==================== patchelf: set RPATH=$ORIGIN ====================
if [ "$OS" = "Linux" ] && command -v patchelf &>/dev/null; then
    echo ""
    echo "Setting RPATH=\$ORIGIN on all bundled libraries..."
    for f in "$OUTPUT_DIR"/*.so "$OUTPUT_DIR"/*.so.*; do
        [ -f "$f" ] && patchelf --force-rpath --set-rpath '$ORIGIN' "$f" 2>/dev/null
    done
    echo "Done"
fi

# ==================== Verification ====================
echo ""
echo "============================================"
echo "Build completed!"
echo "============================================"
echo ""

echo "Output files:"
ls -lh "$OUTPUT_DIR/"*.so "$OUTPUT_DIR/"*.so.* 2>/dev/null
echo ""

if [ "$OS" = "Linux" ]; then
    echo "Dynamic dependencies of $JNI_SO (with bundled libs):"
    LD_LIBRARY_PATH="$OUTPUT_DIR" ldd "$OUTPUT_DIR/$JNI_SO" 2>/dev/null || true
    echo ""
    echo "Dynamic dependencies of $LUMINA_SO (with bundled libs):"
    LD_LIBRARY_PATH="$OUTPUT_DIR" ldd "$OUTPUT_DIR/$LUMINA_SO" 2>/dev/null || true
    echo ""
fi

echo "To package into JAR:"
echo "  mvn -pl paimon-lumina/paimon-lumina-jni package"
