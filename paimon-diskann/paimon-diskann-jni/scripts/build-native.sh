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

echo ""
echo "Native library location:"
ls -la "$OUTPUT_DIR/$(basename "$SRC_LIB")"
echo ""
echo "To package the JAR with native libraries, run:"
echo "  mvn package"
