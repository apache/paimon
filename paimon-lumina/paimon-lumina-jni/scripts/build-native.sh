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
#   LUMINA_ROOT=/path/to/lumina LUMINA_LIBRARY=/path/to/liblumina.so ./build-native.sh
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
echo "LUMINA_ROOT    : $LUMINA_ROOT"
echo "LUMINA_LIBRARY : ${LUMINA_LIBRARY:-(auto-detect)}"
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

CMAKE_EXTRA_ARGS=""
if [ -n "$LUMINA_LIBRARY" ]; then
    CMAKE_EXTRA_ARGS="-DLUMINA_LIBRARY=$LUMINA_LIBRARY"
fi

cmake -DCMAKE_BUILD_TYPE=Release \
      -DLUMINA_ROOT="$LUMINA_ROOT" \
      -DJAVA_HOME="$JAVA_HOME" \
      $CMAKE_EXTRA_ARGS \
      ..

make -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"

# ==================== Install ====================
SO_NAME="libpaimon_lumina_jni.so"
if [ "$OS" = "Darwin" ]; then
    SO_NAME="libpaimon_lumina_jni.dylib"
fi

cp "$BUILD_DIR/$SO_NAME" "$OUTPUT_DIR/$SO_NAME"

# ==================== Resolve real ELF for liblumina.so ====================
# liblumina.so may be a linker script (text) instead of a real ELF binary.
# We need the actual ELF file for runtime dlopen.
resolve_real_so() {
    local so_path="$1"
    if [ ! -f "$so_path" ]; then
        return 1
    fi

    # Check if it's an ELF binary
    if file "$so_path" | grep -q "ELF"; then
        echo "$so_path"
        return 0
    fi

    # It might be a linker script — try to parse the referenced library
    # Common formats: GROUP ( /path/to/lib.so ), INPUT ( lib.so )
    local referenced
    referenced=$(grep -oP '(?<=\( ).*?(?= \))' "$so_path" 2>/dev/null | head -1)
    if [ -z "$referenced" ]; then
        referenced=$(grep -oP '/\S+\.so[\S]*' "$so_path" 2>/dev/null | head -1)
    fi

    if [ -n "$referenced" ]; then
        # If it's a relative path, resolve relative to the .so's directory
        if [[ "$referenced" != /* ]]; then
            local dir
            dir="$(dirname "$so_path")"
            referenced="$dir/$referenced"
        fi
        referenced="$(readlink -f "$referenced" 2>/dev/null || echo "$referenced")"
        if [ -f "$referenced" ] && file "$referenced" | grep -q "ELF"; then
            echo "$referenced"
            return 0
        fi
    fi

    # Last resort: try readlink -f (handles symlink chains)
    local resolved
    resolved="$(readlink -f "$so_path")"
    if [ -f "$resolved" ] && file "$resolved" | grep -q "ELF"; then
        echo "$resolved"
        return 0
    fi

    return 1
}

# ==================== Bundle liblumina.so ====================
LUMINA_SO_TO_BUNDLE=""
if [ -n "$LUMINA_LIBRARY" ] && [[ "$LUMINA_LIBRARY" == *.so* ]]; then
    LUMINA_SO_TO_BUNDLE="$LUMINA_LIBRARY"
else
    for candidate in "$LUMINA_ROOT/lib/liblumina.so" "$LUMINA_ROOT/lib64/liblumina.so"; do
        if [ -f "$candidate" ]; then
            LUMINA_SO_TO_BUNDLE="$candidate"
            break
        fi
    done
fi

if [ -n "$LUMINA_SO_TO_BUNDLE" ]; then
    echo ""
    echo "Resolving liblumina.so..."
    echo "  Input: $LUMINA_SO_TO_BUNDLE"

    LUMINA_SO_REAL="$(resolve_real_so "$LUMINA_SO_TO_BUNDLE")" || true

    if [ -n "$LUMINA_SO_REAL" ] && [ -f "$LUMINA_SO_REAL" ]; then
        echo "  Resolved ELF: $LUMINA_SO_REAL"
        cp "$LUMINA_SO_REAL" "$OUTPUT_DIR/liblumina.so"
        chmod +x "$OUTPUT_DIR/liblumina.so"
        echo "  Bundled: $OUTPUT_DIR/liblumina.so"
        ls -lh "$OUTPUT_DIR/liblumina.so"
    else
        echo "  WARNING: Could not resolve to a real ELF binary!"
        echo "  file $LUMINA_SO_TO_BUNDLE:"
        file "$LUMINA_SO_TO_BUNDLE"
        echo ""
        echo "  If it's a linker script, find the actual .so and set LUMINA_LIBRARY to it."
    fi
fi

# ==================== Bundle liblumina.so dependencies ====================
if [ "$OS" = "Linux" ] && [ -f "$OUTPUT_DIR/liblumina.so" ]; then
    echo ""
    echo "Checking liblumina.so dependencies..."
    MISSING_DEPS=""
    while IFS= read -r line; do
        lib_name=$(echo "$line" | awk '{print $1}')
        lib_path=$(echo "$line" | grep -oP '=> \K/\S+' || true)

        # Skip system libs that are always available
        case "$lib_name" in
            linux-vdso*|libpthread*|libdl*|libm.*|libc.*|librt*|ld-linux*)
                continue ;;
        esac

        if echo "$line" | grep -q "not found"; then
            MISSING_DEPS="$MISSING_DEPS $lib_name"
            continue
        fi

        if [ -n "$lib_path" ] && [ -f "$lib_path" ]; then
            case "$lib_name" in
                libgomp*|libgcc_s*|libstdc++*)
                    if [ ! -f "$OUTPUT_DIR/$lib_name" ]; then
                        echo "  Bundling dependency: $lib_name <- $lib_path"
                        cp "$lib_path" "$OUTPUT_DIR/$lib_name"
                        chmod +x "$OUTPUT_DIR/$lib_name"
                    fi
                    ;;
            esac
        fi
    done < <(ldd "$OUTPUT_DIR/liblumina.so" 2>/dev/null || true)

    if [ -n "$MISSING_DEPS" ]; then
        echo "  WARNING: Missing dependencies:$MISSING_DEPS"
    fi
fi

# ==================== patchelf ====================
if [ "$OS" = "Linux" ] && command -v patchelf &>/dev/null; then
    echo ""
    echo "Setting rpath..."
    for lib in "$OUTPUT_DIR"/*.so*; do
        if [ -f "$lib" ] && file "$lib" | grep -q "ELF"; then
            echo "  patchelf: $(basename "$lib")"
            patchelf --force-rpath --set-rpath \
                '$ORIGIN:/usr/local/lib64:/usr/lib/jvm/java/jre/lib/amd64:/usr/lib/jvm/java/jre/lib/amd64/server' \
                "$lib" 2>/dev/null || true
        fi
    done
    echo "Done"
fi

# ==================== Verification ====================
echo ""
echo "============================================"
echo "Build completed!"
echo "============================================"
echo ""
echo "Bundled files:"
ls -lh "$OUTPUT_DIR/"
echo ""

# Verify all bundled .so are real ELF binaries
VERIFY_OK=true
for lib in "$OUTPUT_DIR"/*.so*; do
    if [ -f "$lib" ]; then
        if file "$lib" | grep -q "ELF"; then
            echo "  OK  $(basename "$lib") — ELF binary"
        else
            echo "  ERR $(basename "$lib") — NOT an ELF binary!"
            file "$lib"
            VERIFY_OK=false
        fi
    fi
done
echo ""

# Show dependency tree for the JNI library
if [ "$OS" = "Linux" ]; then
    echo "Dependency tree for $SO_NAME:"
    ldd "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true
    echo ""

    if [ -f "$OUTPUT_DIR/liblumina.so" ]; then
        echo "Dependency tree for liblumina.so:"
        ldd "$OUTPUT_DIR/liblumina.so" 2>/dev/null || true
        echo ""
    fi

    # Final check: any "not found" in ldd output?
    NOT_FOUND=$(ldd "$OUTPUT_DIR/$SO_NAME" 2>/dev/null | grep "not found" || true)
    if [ -f "$OUTPUT_DIR/liblumina.so" ]; then
        NOT_FOUND="$NOT_FOUND$(ldd "$OUTPUT_DIR/liblumina.so" 2>/dev/null | grep "not found" || true)"
    fi

    if [ -n "$NOT_FOUND" ]; then
        echo "WARNING: Some dependencies are not found on this system:"
        echo "$NOT_FOUND"
        echo ""
        echo "  These may be resolved at runtime via LD_LIBRARY_PATH or by"
        echo "  the NativeLibraryLoader (if bundled in the JAR)."
    else
        echo "All dependencies resolved on this system."
    fi
elif [ "$OS" = "Darwin" ]; then
    echo "Dependencies:"
    otool -L "$OUTPUT_DIR/$SO_NAME" 2>/dev/null || true
fi

if [ "$VERIFY_OK" = false ]; then
    echo ""
    echo "ERROR: Some bundled files are not valid ELF binaries. See above."
    exit 1
fi

echo ""
echo "To package into JAR:"
echo "  mvn -pl paimon-lumina/paimon-lumina-jni package"
