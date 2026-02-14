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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="$PROJECT_DIR/src/main/native"
BUILD_DIR="$PROJECT_DIR/build/native"

# Parse arguments
CLEAN=false
RELEASE=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        --debug)
            RELEASE=false
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --clean                Clean build directory before building"
            echo "  --debug                Build in debug mode (default: release)"
            echo "  --help                 Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  RUST_TARGET            Cargo target triple (e.g. aarch64-apple-darwin)"
            echo "  RUSTFLAGS              Extra rustc flags"
            echo "  CARGO_FEATURES         Extra cargo features (comma-separated)"
            echo ""
            echo "Example:"
            echo "  $0 --clean"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "================================================"
echo "Building Paimon DiskANN JNI - Native Library"
echo "================================================"
echo "Build mode: $([ "$RELEASE" = true ] && echo release || echo debug)"
echo ""

if [ "$CLEAN" = true ]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"
cd "$NATIVE_DIR"

if [ ! -f "$NATIVE_DIR/Cargo.toml" ]; then
    echo "ERROR: Cargo.toml not found in $NATIVE_DIR"
    echo "Place the Rust JNI crate under src/main/native."
    exit 1
fi

# Ensure $HOME/.cargo/bin is in PATH (rustup installs here).
if [ -d "$HOME/.cargo/bin" ]; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# ---------- Check for required tools: rustup, rustc, cargo ----------
# The rust-toolchain.toml in the native crate directory specifies Rust 1.90.0
# which is required by diskann-vector v0.45.0 (uses unsigned_is_multiple_of,
# stabilised in Rust 1.87).
REQUIRED_RUST_VERSION="1.90.0"

# 1. Ensure rustup is available; install if missing.
if ! command -v rustup &> /dev/null; then
    echo ""
    echo "rustup not found. Installing rustup with Rust $REQUIRED_RUST_VERSION..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain "$REQUIRED_RUST_VERSION"
    export PATH="$HOME/.cargo/bin:$PATH"
fi

# 2. Ensure the required toolchain (and its cargo/rustc) is installed.
echo ""
echo "Ensuring the required Rust toolchain ($REQUIRED_RUST_VERSION) is installed..."
if ! rustup run "$REQUIRED_RUST_VERSION" rustc --version &> /dev/null; then
    echo "Toolchain $REQUIRED_RUST_VERSION not found. Installing..."
    rustup toolchain install "$REQUIRED_RUST_VERSION" --profile minimal
fi

# 3. Verify cargo is usable with the required toolchain.
if ! rustup run "$REQUIRED_RUST_VERSION" cargo --version &> /dev/null; then
    echo "cargo not usable with toolchain $REQUIRED_RUST_VERSION. Re-installing..."
    rustup toolchain uninstall "$REQUIRED_RUST_VERSION" 2>/dev/null || true
    rustup toolchain install "$REQUIRED_RUST_VERSION" --profile minimal
fi

echo "  rustc: $(rustup run "$REQUIRED_RUST_VERSION" rustc --version)"
echo "  cargo: $(rustup run "$REQUIRED_RUST_VERSION" cargo --version)"

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)

echo "Detected platform: $OS $ARCH"

# Build with Cargo
echo ""
echo "Building with Cargo..."

CARGO_ARGS=()
if [ "$RELEASE" = true ]; then
    CARGO_ARGS+=(--release)
fi
if [ -n "$RUST_TARGET" ]; then
    CARGO_ARGS+=(--target "$RUST_TARGET")
    echo "Using RUST_TARGET: $RUST_TARGET"
fi
if [ -n "$CARGO_FEATURES" ]; then
    CARGO_ARGS+=(--features "$CARGO_FEATURES")
    echo "Using CARGO_FEATURES: $CARGO_FEATURES"
fi

cargo build "${CARGO_ARGS[@]}"

echo ""
echo "============================================"
echo "Build completed successfully!"
echo "============================================"

# Determine output directory based on platform
if [ "$OS" = "Linux" ]; then
    PLATFORM_OS="linux"
    if [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "amd64" ]; then
        PLATFORM_ARCH="amd64"
    else
        PLATFORM_ARCH="aarch64"
    fi
elif [ "$OS" = "Darwin" ]; then
    PLATFORM_OS="darwin"
    if [ "$ARCH" = "arm64" ]; then
        PLATFORM_ARCH="aarch64"
    else
        PLATFORM_ARCH="amd64"
    fi
else
    echo "Unsupported OS: $OS"
    exit 1
fi

OUTPUT_DIR="$PROJECT_DIR/src/main/resources/$PLATFORM_OS/$PLATFORM_ARCH"
mkdir -p "$OUTPUT_DIR"

LIB_NAME="libpaimon_diskann_jni"
BUILD_MODE_DIR="$([ "$RELEASE" = true ] && echo release || echo debug)"

if [ -n "$RUST_TARGET" ]; then
    TARGET_DIR="target/$RUST_TARGET/$BUILD_MODE_DIR"
else
    TARGET_DIR="target/$BUILD_MODE_DIR"
fi

if [ "$OS" = "Darwin" ]; then
    SRC_LIB="$NATIVE_DIR/$TARGET_DIR/$LIB_NAME.dylib"
else
    SRC_LIB="$NATIVE_DIR/$TARGET_DIR/$LIB_NAME.so"
fi

if [ ! -f "$SRC_LIB" ]; then
    echo "ERROR: Built library not found: $SRC_LIB"
    exit 1
fi

cp "$SRC_LIB" "$OUTPUT_DIR/"

# =====================================================================
# Bundle shared library dependencies
# =====================================================================
# Rust cdylib statically links all Rust code but may dynamically link
# to system C/C++ libraries (libgcc_s, libstdc++, etc.).  On Linux CI
# containers the target machine may have different versions, so we
# bundle all non-trivial dependencies — mirroring the FAISS approach.
# =====================================================================

echo ""
echo "============================================"
echo "Checking & bundling library dependencies"
echo "============================================"

if [ "$OS" = "Linux" ]; then
    # ---- Helper: copy a real library file into OUTPUT_DIR ----
    bundle_lib() {
        local src_path="$1"
        local target_name="$2"

        if [ -f "$OUTPUT_DIR/$target_name" ]; then
            echo "  Already bundled: $target_name"
            return 0
        fi

        # Resolve symlinks to the real file
        local real_path
        real_path=$(readlink -f "$src_path" 2>/dev/null || realpath "$src_path" 2>/dev/null || echo "$src_path")
        if [ ! -f "$real_path" ]; then
            echo "  Cannot resolve: $src_path"
            return 1
        fi

        cp "$real_path" "$OUTPUT_DIR/$target_name"
        chmod +x "$OUTPUT_DIR/$target_name"
        echo "  Bundled: $real_path -> $target_name"
        return 0
    }

    # ---- Helper: search common paths for a library by glob pattern ----
    find_and_bundle() {
        local pattern="$1"
        local target_name="$2"

        if [ -f "$OUTPUT_DIR/$target_name" ]; then
            echo "  Already bundled: $target_name"
            return 0
        fi

        for search_path in /usr/local/lib /usr/local/lib64 \
                           /usr/lib /usr/lib64 \
                           /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu; do
            local found_lib
            found_lib=$(find "$search_path" -maxdepth 1 -name "$pattern" -type f 2>/dev/null | head -1)
            if [ -n "$found_lib" ] && [ -f "$found_lib" ]; then
                bundle_lib "$found_lib" "$target_name"
                return $?
            fi
            local found_link
            found_link=$(find "$search_path" -maxdepth 1 -name "$pattern" -type l 2>/dev/null | head -1)
            if [ -n "$found_link" ] && [ -L "$found_link" ]; then
                bundle_lib "$found_link" "$target_name"
                return $?
            fi
        done

        # Try ldconfig cache
        local ldconfig_path
        ldconfig_path=$(ldconfig -p 2>/dev/null | grep "$pattern" | head -1 | awk '{print $NF}')
        if [ -n "$ldconfig_path" ] && [ -f "$ldconfig_path" ]; then
            bundle_lib "$ldconfig_path" "$target_name"
            return $?
        fi

        echo "  Not found: $pattern"
        return 1
    }

    echo ""
    echo "Bundling required libraries..."

    # 1. GCC runtime (Rust cdylib may link against libgcc_s for stack unwinding)
    if ! find_and_bundle "libgcc_s.so*" "libgcc_s.so.1"; then
        echo "  Note: libgcc_s not found as shared library - likely statically linked"
    fi

    # 2. C++ standard library (needed if the diskann crate compiles any C++ code)
    if ! find_and_bundle "libstdc++.so*" "libstdc++.so.6"; then
        echo "  Note: libstdc++ not found as shared library - likely statically linked"
    fi

    # ---- Scan ldd for additional non-system dependencies ----
    echo ""
    echo "Scanning ldd for additional dependencies..."
    JNI_LIB="$OUTPUT_DIR/$(basename "$SRC_LIB")"
    LIBS_TO_CHECK="$JNI_LIB"
    for bundled_lib in "$OUTPUT_DIR"/*.so*; do
        [ -f "$bundled_lib" ] && LIBS_TO_CHECK="$LIBS_TO_CHECK $bundled_lib"
    done

    LIBS_CHECKED=""
    while [ -n "$LIBS_TO_CHECK" ]; do
        CURRENT_LIB=$(echo "$LIBS_TO_CHECK" | awk '{print $1}')
        LIBS_TO_CHECK=$(echo "$LIBS_TO_CHECK" | cut -d' ' -f2-)
        [ "$LIBS_TO_CHECK" = "$CURRENT_LIB" ] && LIBS_TO_CHECK=""

        # Skip already-checked
        echo "$LIBS_CHECKED" | grep -q "$CURRENT_LIB" 2>/dev/null && continue
        LIBS_CHECKED="$LIBS_CHECKED $CURRENT_LIB"

        [ ! -f "$CURRENT_LIB" ] && continue

        echo "  Checking deps of: $(basename "$CURRENT_LIB")"

        DEPS=$(ldd "$CURRENT_LIB" 2>/dev/null | grep "=>" | awk '{print $1 " " $3}') || true

        while IFS= read -r dep_line; do
            [ -z "$dep_line" ] && continue
            DEP_NAME=$(echo "$dep_line" | awk '{print $1}')
            DEP_PATH=$(echo "$dep_line" | awk '{print $2}')

            # Skip universally-available system libraries
            case "$DEP_NAME" in
                linux-vdso.so*|libc.so*|libm.so*|libpthread.so*|libdl.so*|librt.so*|ld-linux*)
                    continue
                    ;;
            esac

            # Bundle known problematic libraries
            case "$DEP_NAME" in
                libgcc_s*)
                    bundle_lib "$DEP_PATH" "libgcc_s.so.1" || true
                    ;;
                libstdc++*)
                    if bundle_lib "$DEP_PATH" "libstdc++.so.6"; then
                        LIBS_TO_CHECK="$LIBS_TO_CHECK $OUTPUT_DIR/libstdc++.so.6"
                    fi
                    ;;
                libgomp*)
                    if bundle_lib "$DEP_PATH" "libgomp.so.1"; then
                        LIBS_TO_CHECK="$LIBS_TO_CHECK $OUTPUT_DIR/libgomp.so.1"
                    fi
                    ;;
                libquadmath*)
                    bundle_lib "$DEP_PATH" "libquadmath.so.0" || true
                    ;;
                libgfortran*)
                    bundle_lib "$DEP_PATH" "libgfortran.so.3" || true
                    ;;
            esac
        done <<< "$DEPS"
    done

    # ---- Set rpath to $ORIGIN so bundled libs are found at load time ----
    if command -v patchelf &>/dev/null; then
        echo ""
        echo "Setting rpath to \$ORIGIN for all libraries..."
        for lib in "$OUTPUT_DIR"/*.so*; do
            if [ -f "$lib" ]; then
                patchelf --set-rpath '$ORIGIN' "$lib" 2>/dev/null || true
            fi
        done
        echo "Done setting rpath"
    else
        echo ""
        echo "WARNING: patchelf not found, cannot set rpath."
        echo "         Install with: sudo apt-get install patchelf"
        echo "         The Java loader will still pre-load deps from JAR, but setting"
        echo "         rpath provides an additional safety net."
    fi

elif [ "$OS" = "Darwin" ]; then
    # On macOS, Rust cdylibs are normally self-contained.
    # But check if any non-system dylibs are referenced.
    echo ""
    echo "Checking macOS dylib dependencies..."
    DYLIB_PATH="$OUTPUT_DIR/$(basename "$SRC_LIB")"
    otool -L "$DYLIB_PATH" 2>/dev/null | tail -n +2 | while read -r dep_entry; do
        dep_path=$(echo "$dep_entry" | awk '{print $1}')
        case "$dep_path" in
            /usr/lib/*|/System/*|@rpath/*|@loader_path/*|@executable_path/*)
                # System or relative — OK
                ;;
            *)
                if [ -f "$dep_path" ]; then
                    dep_basename=$(basename "$dep_path")
                    if [ ! -f "$OUTPUT_DIR/$dep_basename" ]; then
                        echo "  Bundling macOS dep: $dep_path -> $dep_basename"
                        cp "$dep_path" "$OUTPUT_DIR/$dep_basename"
                        chmod +x "$OUTPUT_DIR/$dep_basename"
                        # Rewrite the install name so the JNI lib finds the bundled copy
                        install_name_tool -change "$dep_path" "@loader_path/$dep_basename" "$DYLIB_PATH" 2>/dev/null || true
                    fi
                fi
                ;;
        esac
    done
fi

# =====================================================================
# Summary: list all libraries and their dependencies
# =====================================================================

echo ""
echo "============================================"
echo "Native library summary"
echo "============================================"

BUILT_LIBS=$(find "$PROJECT_DIR/src/main/resources" -type f \( -name "*.so" -o -name "*.so.*" -o -name "*.dylib" \) 2>/dev/null)

if [ -n "$BUILT_LIBS" ]; then
    for lib in $BUILT_LIBS; do
        echo ""
        echo "Library: $lib"
        ls -la "$lib"

        echo ""
        echo "Dependencies:"
        if [ "$OS" = "Darwin" ]; then
            otool -L "$lib" 2>/dev/null | head -20 || true
        elif [ "$OS" = "Linux" ]; then
            ldd "$lib" 2>/dev/null | head -20 || readelf -d "$lib" 2>/dev/null | grep NEEDED | head -20 || true
        fi
    done
else
    echo "  (no libraries found)"
    ls -la "$PROJECT_DIR/src/main/resources/"*/*/ 2>/dev/null || true
fi

echo ""
echo "To package the JAR with native libraries, run:"
echo "  mvn package"
