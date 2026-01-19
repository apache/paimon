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
FAISS_VERSION="1.7.4"  # Default FAISS version

while [[ $# -gt 0 ]]; do
    case $1 in
        --opt-level)
            OPT_LEVEL="$2"
            shift 2
            ;;
        --faiss-version)
            FAISS_VERSION="$2"
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
            echo "  --opt-level LEVEL      Optimization level: generic, avx2, avx512 (default: generic)"
            echo "  --faiss-version VER    FAISS version to use (default: 1.7.4)"
            echo "  --fat-lib              Build fat library with all dependencies (default: enabled)"
            echo "  --no-fat-lib           Build without bundling dependencies"
            echo "  --clean                Clean build directory before building"
            echo "  --help                 Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  FAISS_ROOT             Path to Faiss installation"
            echo "  JAVA_HOME              Path to Java installation"
            echo "  OPENBLAS_ROOT          Path to OpenBLAS installation"
            echo ""
            echo "Example:"
            echo "  FAISS_ROOT=/opt/faiss $0 --clean --fat-lib --faiss-version 1.8.0"
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
echo "FAISS version: $FAISS_VERSION"
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
    -DFAISS_VERSION="$FAISS_VERSION"
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
fi
OUTPUT_DIR="$PROJECT_DIR/src/main/resources/$PLATFORM_OS/$PLATFORM_ARCH"

# Bundle dependency libraries if building fat lib and they're dynamically linked
if [ "$FAT_LIB" = true ] && [ "$OS" = "Linux" ]; then
    echo ""
    echo "Checking for dynamic dependencies to bundle..."
    
    JNI_LIB="$OUTPUT_DIR/libpaimon_faiss_jni.so"
    if [ -f "$JNI_LIB" ]; then
        # Function to bundle a library
        bundle_lib() {
            local lib_path="$1"
            local target_name="$2"
            
            if [ -z "$lib_path" ] || [ ! -f "$lib_path" ]; then
                return 1
            fi
            
            local real_path=$(readlink -f "$lib_path")
            if [ -f "$OUTPUT_DIR/$target_name" ]; then
                echo "  Already bundled: $target_name"
                return 0
            fi
            
            cp "$real_path" "$OUTPUT_DIR/$target_name"
            chmod +x "$OUTPUT_DIR/$target_name"
            echo "  Bundled: $real_path -> $target_name"
            return 0
        }
        
        # Function to find and bundle a library by name pattern
        find_and_bundle() {
            local pattern="$1"
            local target_name="$2"
            
            if [ -f "$OUTPUT_DIR/$target_name" ]; then
                echo "  Already bundled: $target_name"
                return 0
            fi
            
            # Search in common library paths (order matters - prefer /usr/local for user-installed libs)
            for search_path in /usr/local/lib /usr/local/lib64 \
                              /usr/lib /usr/lib64 \
                              /usr/lib/x86_64-linux-gnu /usr/lib/aarch64-linux-gnu \
                              /usr/lib/x86_64-linux-gnu/openblas-pthread \
                              /usr/lib/aarch64-linux-gnu/openblas-pthread; do
                local found_lib=$(find "$search_path" -maxdepth 1 -name "$pattern" -type f 2>/dev/null | head -1)
                if [ -n "$found_lib" ] && [ -f "$found_lib" ]; then
                    bundle_lib "$found_lib" "$target_name"
                    return $?
                fi
                # Also check for symlinks
                local found_link=$(find "$search_path" -maxdepth 1 -name "$pattern" -type l 2>/dev/null | head -1)
                if [ -n "$found_link" ] && [ -L "$found_link" ]; then
                    bundle_lib "$found_link" "$target_name"
                    return $?
                fi
            done
            
            # Try ldconfig cache
            local ldconfig_path=$(ldconfig -p 2>/dev/null | grep "$pattern" | head -1 | awk '{print $NF}')
            if [ -n "$ldconfig_path" ] && [ -f "$ldconfig_path" ]; then
                bundle_lib "$ldconfig_path" "$target_name"
                return $?
            fi
            
            echo "  Not found: $pattern"
            return 1
        }
        
        echo ""
        echo "Bundling required libraries..."
        
        # Explicitly bundle FAISS and its dependencies (in dependency order)
        # These may not show up in ldd if statically linked
        # Use || true to prevent set -e from failing on missing libraries
        
        # 1. GCC runtime
        if ! find_and_bundle "libgcc_s.so*" "libgcc_s.so.1"; then
           echo "  Note: libgcc_s not found as shared library - likely statically linked"
        fi
        
        # 2. Quadmath (needed by gfortran)
        if ! find_and_bundle "libquadmath.so*" "libquadmath.so.0"; then
            echo "  Note: libquadmath not found as shared library - likely statically linked"
        fi

        # 3. Fortran runtime (needed by OpenBLAS)
        if ! find_and_bundle "libgfortran.so*" "libgfortran.so.3"; then
           echo "  Note: libgfortran not found as shared library - likely statically linked"
        fi
        # 4. OpenMP runtime
        if ! find_and_bundle "libgomp.so*" "libgomp.so.1"; then
           echo "  Note: libgomp not found as shared library - likely statically linked"
        fi
        
        # 5. BLAS/LAPACK
        if ! find_and_bundle "libblas.so*" "libblas.so.3"; then
           echo "  Note: libblas not found as shared library - likely statically linked"
        fi
        if ! find_and_bundle "liblapack.so*" "liblapack.so.3"; then
           echo "  Note: liblapack not found as shared library - likely statically linked"
        fi
        
        # 6. OpenBLAS
        if ! find_and_bundle "libopenblas*.so*" "libopenblas.so.0"; then
           echo "  Note: libopenblas not found as shared library - likely statically linked"
        fi

        # 7. FAISS library (may be statically linked)
        if ! find_and_bundle "libfaiss.so*" "libfaiss.so"; then
            echo "  Note: libfaiss not found as shared library - likely statically linked"
        fi
        
        # Also check ldd for any additional dependencies we might have missed
        echo ""
        echo "Checking ldd for additional dependencies..."
        LIBS_TO_CHECK="$JNI_LIB"
        for bundled_lib in "$OUTPUT_DIR"/*.so*; do
            [ -f "$bundled_lib" ] && LIBS_TO_CHECK="$LIBS_TO_CHECK $bundled_lib"
        done
        
        LIBS_CHECKED=""
        while [ -n "$LIBS_TO_CHECK" ]; do
            CURRENT_LIB=$(echo "$LIBS_TO_CHECK" | awk '{print $1}')
            LIBS_TO_CHECK=$(echo "$LIBS_TO_CHECK" | cut -d' ' -f2-)
            [ "$LIBS_TO_CHECK" = "$CURRENT_LIB" ] && LIBS_TO_CHECK=""
            
            # Skip if already checked
            echo "$LIBS_CHECKED" | grep -q "$CURRENT_LIB" && continue
            LIBS_CHECKED="$LIBS_CHECKED $CURRENT_LIB"
            
            [ ! -f "$CURRENT_LIB" ] && continue
            
            echo "Checking dependencies of: $(basename "$CURRENT_LIB")"
            
            # Get all dependencies
            DEPS=$(ldd "$CURRENT_LIB" 2>/dev/null | grep "=>" | awk '{print $1 " " $3}')
            
            while IFS= read -r dep_line; do
                [ -z "$dep_line" ] && continue
                DEP_NAME=$(echo "$dep_line" | awk '{print $1}')
                DEP_PATH=$(echo "$dep_line" | awk '{print $2}')
                
                # Skip system libraries that are universally available
                case "$DEP_NAME" in
                    linux-vdso.so*|libc.so*|libm.so*|libpthread.so*|libdl.so*|librt.so*|ld-linux*|libstdc++*)
                        continue
                        ;;
                esac
                
                # Bundle specific libraries we know are problematic
                case "$DEP_NAME" in
                    libfaiss*)
                        if bundle_lib "$DEP_PATH" "libfaiss.so"; then
                            LIBS_TO_CHECK="$LIBS_TO_CHECK $OUTPUT_DIR/libfaiss.so"
                        fi
                        ;;
                    libopenblas*)
                        if bundle_lib "$DEP_PATH" "libopenblas.so.0"; then
                            LIBS_TO_CHECK="$LIBS_TO_CHECK $OUTPUT_DIR/libopenblas.so.0"
                        fi
                        ;;
                    libgfortran*)
                        bundle_lib "$DEP_PATH" "libgfortran.so.3"
                        ;;
                    libgomp*)
                        bundle_lib "$DEP_PATH" "libgomp.so.1"
                        ;;
                    libquadmath*)
                        bundle_lib "$DEP_PATH" "libquadmath.so.0"
                        ;;
                    libgcc_s*)
                        bundle_lib "$DEP_PATH" "libgcc_s.so.1"
                        ;;
                    libblas*)
                        bundle_lib "$DEP_PATH" "libblas.so.3"
                        ;;
                    liblapack*)
                        bundle_lib "$DEP_PATH" "liblapack.so.3"
                        ;;
                esac
            done <<< "$DEPS"
        done
        
        # Set rpath to $ORIGIN for all bundled libraries
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
            echo "WARNING: patchelf not found, cannot set rpath"
            echo "Install with: sudo apt-get install patchelf"
        fi
    fi
fi

echo ""
echo "Native library location:"
BUILT_LIBS=$(find "$PROJECT_DIR/src/main/resources" -type f \( -name "*.so" -o -name "*.so.*" -o -name "*.dylib" \) 2>/dev/null)

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

