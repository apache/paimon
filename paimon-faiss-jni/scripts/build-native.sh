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
OPT_LEVEL="generic"
CLEAN=false
FAT_LIB=true  # Default to fat lib

while [[ $# -gt 0 ]]; do
    case $1 in
        --opt-level)
            OPT_LEVEL="$2"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --fat-lib)
            FAT_LIB=true
            shift
            ;;
        --no-fat-lib)
            FAT_LIB=false
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --opt-level LEVEL   Optimization level: generic, avx2, avx512 (default: generic)"
            echo "  --fat-lib           Build fat library with all dependencies (default: enabled)"
            echo "  --no-fat-lib        Build without bundling dependencies"
            echo "  --clean             Clean build directory before building"
            echo "  --help              Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  FAISS_ROOT          Path to Faiss installation"
            echo "  JAVA_HOME           Path to Java installation"
            echo "  OPENBLAS_ROOT       Path to OpenBLAS installation"
            echo ""
            echo "Example:"
            echo "  FAISS_ROOT=/opt/faiss $0 --clean --fat-lib"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "================================================"
echo "Building Paimon Faiss JNI - Native Library"
echo "================================================"
echo "Optimization level: $OPT_LEVEL"
echo "Fat library: $FAT_LIB"
echo ""

# Clean if requested
if [ "$CLEAN" = true ]; then
    echo "Cleaning build directory..."
    rm -rf "$BUILD_DIR"
fi

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Check for CMake cache from different source directory (cross-machine builds)
if [ -f "CMakeCache.txt" ]; then
    CACHED_SOURCE=$(grep "CMAKE_HOME_DIRECTORY:INTERNAL=" CMakeCache.txt 2>/dev/null | cut -d'=' -f2)
    if [ -n "$CACHED_SOURCE" ] && [ "$CACHED_SOURCE" != "$NATIVE_DIR" ]; then
        echo "Detected CMake cache from different source directory."
        echo "  Cached: $CACHED_SOURCE"
        echo "  Current: $NATIVE_DIR"
        echo "Cleaning build directory to avoid conflicts..."
        rm -rf "$BUILD_DIR"/*
    fi
fi

# Detect platform
OS=$(uname -s)
ARCH=$(uname -m)

echo "Detected platform: $OS $ARCH"

# macOS specific: check for libomp
if [ "$OS" = "Darwin" ]; then
    if ! brew list libomp &>/dev/null; then
        echo ""
        echo "WARNING: libomp not found. Installing via Homebrew..."
        echo "Run: brew install libomp"
        echo ""
        echo "If you don't have Homebrew, install it from https://brew.sh"
        echo "Or install libomp manually and set OPENMP_ROOT environment variable."
        echo ""
        
        # Try to install automatically
        if command -v brew &>/dev/null; then
            brew install libomp
        else
            echo "ERROR: Homebrew not found. Please install libomp manually."
            exit 1
        fi
    else
        echo "Found libomp via Homebrew"
    fi
fi

# Run CMake
echo ""
echo "Configuring with CMake..."

CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DFAISS_OPT_LEVEL="$OPT_LEVEL"
    -DBUILD_FAT_LIB="$FAT_LIB"
)

# Add platform-specific options
if [ "$OS" = "Darwin" ]; then
    # On macOS, we might need to specify the SDK
    if [ -n "$SDKROOT" ]; then
        CMAKE_ARGS+=(-DCMAKE_OSX_SYSROOT="$SDKROOT")
    fi
    
    # For Apple Silicon, we might want universal binary
    if [ "$ARCH" = "arm64" ]; then
        CMAKE_ARGS+=(-DCMAKE_OSX_ARCHITECTURES="arm64")
    fi
fi

# If FAISS_ROOT is set, pass it to CMake
if [ -n "$FAISS_ROOT" ]; then
    CMAKE_ARGS+=(-DFAISS_ROOT="$FAISS_ROOT")
    echo "Using FAISS_ROOT: $FAISS_ROOT"
fi

# If OPENBLAS_ROOT is set, pass it to CMake
if [ -n "$OPENBLAS_ROOT" ]; then
    CMAKE_ARGS+=(-DOPENBLAS_ROOT="$OPENBLAS_ROOT")
    echo "Using OPENBLAS_ROOT: $OPENBLAS_ROOT"
fi

# If JAVA_HOME is set, use it
if [ -n "$JAVA_HOME" ]; then
    CMAKE_ARGS+=(-DJAVA_HOME="$JAVA_HOME")
    echo "Using JAVA_HOME: $JAVA_HOME"
fi

cmake "${CMAKE_ARGS[@]}" "$NATIVE_DIR"

# Build
echo ""
echo "Building..."
cmake --build . --config Release -j "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

echo ""
echo "============================================"
echo "Build completed successfully!"
echo "============================================"
echo ""
echo "Native library location:"
BUILT_LIBS=$(find "$PROJECT_DIR/src/main/resources" -type f \( -name "*.so" -o -name "*.dylib" \) 2>/dev/null)

if [ -n "$BUILT_LIBS" ]; then
    for lib in $BUILT_LIBS; do
        echo ""
        echo "Library: $lib"
        ls -la "$lib"
        
        # Show library dependencies
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

