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
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script to build the FAISS JNI native library
#
# Usage:
#   ./build.sh              # Build FAT library with all dependencies bundled (default)
#   ./build.sh --dynamic    # Build with dynamic linking (requires libfaiss.so at runtime)
#
# Prerequisites for FAT library build (Ubuntu/Debian):
#   sudo apt-get install -y build-essential cmake libopenblas-dev liblapack-dev gfortran
#
# Prerequisites for FAT library build (CentOS/RHEL):
#   sudo yum install -y gcc-c++ cmake openblas-devel lapack-devel gcc-gfortran
#
# Build FAISS from source with static libraries:
#   git clone https://github.com/facebookresearch/faiss.git
#   cd faiss
#   cmake -B build -DBUILD_SHARED_LIBS=OFF -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DCMAKE_BUILD_TYPE=Release .
#   cmake --build build -j$(nproc)
#   export FAISS_HOME=$(pwd)
#   export FAISS_BUILD_DIR=$(pwd)/build

set -e

# Parse arguments
FAT_LIBRARY=true
for arg in "$@"; do
    case $arg in
        --dynamic)
            FAT_LIBRARY=false
            shift
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# Detect platform
OS_NAME=$(uname -s)
ARCH=$(uname -m)

case "$OS_NAME" in
    Linux*)
        PLATFORM="linux"
        ;;
    Darwin*)
        PLATFORM="darwin"
        ;;
    *)
        echo "Unsupported OS: $OS_NAME"
        exit 1
        ;;
esac

case "$ARCH" in
    x86_64|amd64)
        ARCH_NAME="x86_64"
        ;;
    aarch64|arm64)
        ARCH_NAME="aarch64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

NATIVE_OUTPUT_DIR="${SCRIPT_DIR}/../resources/native/${PLATFORM}-${ARCH_NAME}"

echo ""
echo "============================================================"
if [ "$FAT_LIBRARY" = true ]; then
    echo "  Building FAT library for ${PLATFORM}-${ARCH_NAME}"
    echo "  All dependencies will be bundled into a single .so file"
    echo "  No external libraries required at runtime!"
else
    echo "  Building with DYNAMIC linking for ${PLATFORM}-${ARCH_NAME}"
    echo "  libfaiss.so and dependencies required at runtime"
fi
echo "============================================================"
echo ""

# Check dependencies on macOS
if [ "$PLATFORM" = "darwin" ]; then
    echo "Checking macOS dependencies..."
    
    # On macOS, we use dynamic linking via Homebrew
    if ! brew list libomp &>/dev/null; then
        echo "Installing libomp (required for OpenMP support)..."
        brew install libomp
    fi
    
    if ! brew list faiss &>/dev/null; then
        echo "Installing faiss..."
        brew install faiss
    fi
    
    # macOS doesn't support fat library build the same way
    if [ "$FAT_LIBRARY" = true ]; then
        echo "Note: On macOS, using Homebrew faiss with rpath"
    fi
fi

# Check dependencies on Linux
if [ "$PLATFORM" = "linux" ]; then
    echo "Checking Linux dependencies..."
    
    # Check GCC version
    GCC_VERSION=$(gcc -dumpversion 2>/dev/null | cut -d. -f1)
    if [ -n "$GCC_VERSION" ] && [ "$GCC_VERSION" -lt 5 ]; then
        echo "WARNING: GCC version $GCC_VERSION is too old. C++14 or later requires GCC 5+."
        echo "Please install a newer GCC:"
        echo "  On Ubuntu/Debian: sudo apt-get install g++-7 (or newer)"
        echo "  On CentOS/RHEL: sudo yum install devtoolset-7-gcc-c++ && scl enable devtoolset-7 bash"
    fi
    
    # Auto-detect FAISS installation paths
    if [ -z "$FAISS_HOME" ]; then
        # For fat library, require static FAISS
        if [ "$FAT_LIBRARY" = true ]; then
            # Check for static library
            if [ -f /root/faiss/faiss/build/faiss/libfaiss.a ]; then
                export FAISS_HOME="/root/faiss/faiss"
                export FAISS_BUILD_DIR="/root/faiss/faiss/build"
                echo "Found static FAISS at: $FAISS_HOME"
            elif [ -f "${HOME}/faiss/build/faiss/libfaiss.a" ]; then
                export FAISS_HOME="${HOME}/faiss"
                export FAISS_BUILD_DIR="${HOME}/faiss/build"
                echo "Found static FAISS at: $FAISS_HOME"
            elif [ -f /usr/local/lib/libfaiss.a ]; then
                export FAISS_HOME="/usr/local"
                echo "Found static FAISS at: $FAISS_HOME"
            else
                echo ""
                echo "ERROR: Static FAISS library (libfaiss.a) not found!"
                echo ""
                echo "To build a FAT library, you need to build FAISS from source with static libraries:"
                echo ""
                echo "  # Install dependencies first"
                echo "  sudo apt-get install -y build-essential cmake libopenblas-dev liblapack-dev gfortran"
                echo ""
                echo "  # Clone and build FAISS"
                echo "  git clone https://github.com/facebookresearch/faiss.git"
                echo "  cd faiss"
                echo "  cmake -B build -DBUILD_SHARED_LIBS=OFF -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DCMAKE_BUILD_TYPE=Release ."
                echo "  cmake --build build -j\$(nproc)"
                echo ""
                echo "  # Set environment and build JNI"
                echo "  export FAISS_HOME=\$(pwd)"
                echo "  export FAISS_BUILD_DIR=\$(pwd)/build"
                echo "  cd /path/to/paimon-faiss/src/main/native"
                echo "  ./build.sh"
                echo ""
                exit 1
            fi
        else
            # For dynamic linking, look for shared library
            if [ -f /root/faiss/faiss/build/faiss/libfaiss.so ]; then
                export FAISS_HOME="/root/faiss/faiss"
                export FAISS_BUILD_DIR="/root/faiss/faiss/build"
            elif [ -f /usr/local/lib/libfaiss.so ]; then
                export FAISS_HOME="/usr/local"
            elif [ -f /usr/lib/libfaiss.so ] || [ -f /usr/lib64/libfaiss.so ]; then
                export FAISS_HOME="/usr"
            else
                echo "FAISS library not found. Please install FAISS or build from source."
                exit 1
            fi
            echo "Found FAISS at: $FAISS_HOME"
        fi
    else
        echo "Using FAISS_HOME: $FAISS_HOME"
    fi
    
    # Check for required static libraries for fat build
    if [ "$FAT_LIBRARY" = true ]; then
        echo ""
        echo "Checking static library dependencies..."
        
        MISSING_DEPS=""
        
        # Helper function to find static library
        find_static_lib() {
            local lib_name=$1
            local search_paths=(
                "/usr/lib"
                "/usr/lib64"
                "/usr/lib/x86_64-linux-gnu"
                "/usr/lib/aarch64-linux-gnu"
                "/usr/local/lib"
                "/opt/OpenBLAS/lib"
            )
            for path in "${search_paths[@]}"; do
                if [ -f "${path}/lib${lib_name}.a" ]; then
                    echo "${path}/lib${lib_name}.a"
                    return 0
                fi
            done
            return 1
        }
        
        # Check for static OpenBLAS (use || true to prevent set -e from exiting)
        OPENBLAS_A=$(find_static_lib "openblas" 2>/dev/null) || true
        if [ -n "$OPENBLAS_A" ]; then
            echo "  [✓] libopenblas.a found: $OPENBLAS_A"
        else
            echo "  [!] libopenblas.a not found (will try dynamic linking)"
            MISSING_DEPS="$MISSING_DEPS openblas"
        fi
        
        # Check for static LAPACK
        LAPACK_A=$(find_static_lib "lapack" 2>/dev/null) || true
        if [ -n "$LAPACK_A" ]; then
            echo "  [✓] liblapack.a found: $LAPACK_A"
        else
            echo "  [!] liblapack.a not found (optional)"
        fi
        
        # Check for static BLAS (separate from OpenBLAS)
        BLAS_A=$(find_static_lib "blas" 2>/dev/null) || true
        if [ -n "$BLAS_A" ]; then
            echo "  [✓] libblas.a found: $BLAS_A"
        fi
        
        # Note: GCC runtime libraries (gfortran, gomp, quadmath) are always dynamically
        # linked because their static versions often lack -fPIC, especially on aarch64
        echo "  [i] libgfortran - will use dynamic linking (libgfortran.so)"
        echo "  [i] libgomp (OpenMP) - will use dynamic linking (libgomp.so)"
        
        if [ -n "$MISSING_DEPS" ]; then
            echo ""
            echo "Some static libraries not found. Install with:"
            echo "  Ubuntu/Debian: sudo apt-get install -y libopenblas-dev liblapack-dev gfortran"
            echo "  CentOS/RHEL: sudo yum install -y openblas-static lapack-static gcc-gfortran"
            echo ""
            echo "Continuing build (will use dynamic linking for missing libs)..."
        fi
        echo ""
    fi
fi

# Clean and create build directory
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Build CMake arguments
CMAKE_ARGS="-DCMAKE_BUILD_TYPE=Release"

# Add FAISS paths if set
if [ -n "$FAISS_HOME" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DFAISS_HOME=${FAISS_HOME}"
fi
if [ -n "$FAISS_BUILD_DIR" ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DFAISS_BUILD_DIR=${FAISS_BUILD_DIR}"
fi

# Set fat library option
if [ "$FAT_LIBRARY" = true ]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DFAT_LIBRARY=ON"
else
    CMAKE_ARGS="${CMAKE_ARGS} -DFAT_LIBRARY=OFF"
fi

# Configure with CMake
echo ""
echo "Running: cmake ${CMAKE_ARGS} .."
echo ""
cmake ${CMAKE_ARGS} ..

# Build
echo ""
echo "Building..."
cmake --build . --config Release -- -j$(nproc 2>/dev/null || echo 4)

# Create output directory
mkdir -p "${NATIVE_OUTPUT_DIR}"

# Copy the JNI library
if [ "$PLATFORM" = "darwin" ]; then
    cp "${BUILD_DIR}/lib/libpaimon_faiss_jni.dylib" "${NATIVE_OUTPUT_DIR}/"
    echo ""
    echo "Library copied to: ${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.dylib"
    
    # Show dependencies
    echo ""
    echo "Library dependencies (otool -L):"
    otool -L "${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.dylib"
else
    cp "${BUILD_DIR}/lib/libpaimon_faiss_jni.so" "${NATIVE_OUTPUT_DIR}/"
    echo ""
    echo "Library copied to: ${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.so"
    
    # Get library size
    LIB_SIZE=$(du -h "${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.so" | cut -f1)
    echo "Library size: ${LIB_SIZE}"
    
    # Show dependencies
    echo ""
    echo "Checking library dependencies..."
    echo ""
    
    DEPS=$(ldd "${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.so" 2>/dev/null || echo "ldd not available")
    echo "$DEPS"
    
    # Check what's bundled vs external
    echo ""
    echo "============================================================"
    echo "  Dependency Analysis"
    echo "============================================================"
    
    # Check for faiss
    if echo "$DEPS" | grep -q "libfaiss.so"; then
        echo "  [✗] libfaiss.so - EXTERNAL (not bundled)"
        FULLY_BUNDLED=false
    else
        echo "  [✓] FAISS - bundled"
        FULLY_BUNDLED=true
    fi
    
    # Check for openblas
    if echo "$DEPS" | grep -q "libopenblas"; then
        echo "  [✗] libopenblas - EXTERNAL"
        FULLY_BUNDLED=false
    else
        echo "  [✓] OpenBLAS - bundled or not needed"
    fi
    
    # Check for lapack
    if echo "$DEPS" | grep -q "liblapack"; then
        echo "  [✗] liblapack - EXTERNAL"
        FULLY_BUNDLED=false
    else
        echo "  [✓] LAPACK - bundled or not needed"
    fi
    
    # Check for gomp
    if echo "$DEPS" | grep -q "libgomp"; then
        echo "  [!] libgomp - EXTERNAL (OpenMP, usually available)"
    else
        echo "  [✓] GOMP - bundled or not needed"
    fi
    
    # Check for gfortran
    if echo "$DEPS" | grep -q "libgfortran"; then
        echo "  [!] libgfortran - EXTERNAL (usually available)"
    else
        echo "  [✓] gfortran - bundled or not needed"
    fi
    
    echo "============================================================"
    
    if [ "$FULLY_BUNDLED" = true ]; then
        echo ""
        echo "  ✓ SUCCESS: FAT library built successfully!"
        echo "    The library bundles all major dependencies."
        echo "    Only standard system libraries (libc, libm, libpthread,"
        echo "    libdl, libgomp, libgfortran) are required at runtime."
        echo ""
    else
        if [ "$FAT_LIBRARY" = true ]; then
            echo ""
            echo "  ⚠ WARNING: Some dependencies were not bundled."
            echo "    You may need to copy the missing .so files alongside"
            echo "    the JNI library, or install them on target systems."
            echo ""
            
            # Copy any missing shared libraries
            echo "Attempting to copy missing dependencies..."
            
            # Copy libfaiss.so if needed
            if echo "$DEPS" | grep -q "libfaiss.so"; then
                FAISS_SO=$(echo "$DEPS" | grep "libfaiss.so" | awk '{print $3}')
                if [ -f "$FAISS_SO" ]; then
                    cp "$FAISS_SO" "${NATIVE_OUTPUT_DIR}/"
                    echo "  Copied: libfaiss.so"
                fi
            fi
            
            # Copy libopenblas if needed
            if echo "$DEPS" | grep -q "libopenblas"; then
                OPENBLAS_SO=$(echo "$DEPS" | grep "libopenblas" | awk '{print $3}')
                if [ -f "$OPENBLAS_SO" ]; then
                    cp "$OPENBLAS_SO" "${NATIVE_OUTPUT_DIR}/"
                    echo "  Copied: $(basename $OPENBLAS_SO)"
                fi
            fi
        fi
    fi
fi

# Copy dependent shared libraries that couldn't be statically linked
if [ "$PLATFORM" = "linux" ] && [ "$FAT_LIBRARY" = true ]; then
    echo ""
    echo "Copying dependent shared libraries..."
    
    # Get list of dependencies
    DEPS=$(ldd "${NATIVE_OUTPUT_DIR}/libpaimon_faiss_jni.so" 2>/dev/null || echo "")
    
    # Function to copy a library and its symlinks
    copy_lib() {
        local lib_path=$1
        local lib_name=$(basename "$lib_path")
        
        if [ -f "$lib_path" ]; then
            # Copy the actual library file
            cp -L "$lib_path" "${NATIVE_OUTPUT_DIR}/" 2>/dev/null || true
            echo "  Copied: $lib_name"
            
            # If it's a symlink, also get the real file
            if [ -L "$lib_path" ]; then
                local real_path=$(readlink -f "$lib_path")
                local real_name=$(basename "$real_path")
                if [ "$real_name" != "$lib_name" ] && [ -f "$real_path" ]; then
                    cp "$real_path" "${NATIVE_OUTPUT_DIR}/" 2>/dev/null || true
                    echo "  Copied: $real_name (real file)"
                fi
            fi
        fi
    }
    
    # Copy libopenblas if needed
    if echo "$DEPS" | grep -q "libopenblas"; then
        OPENBLAS_SO=$(echo "$DEPS" | grep "libopenblas" | awk '{print $3}' | head -1)
        if [ -n "$OPENBLAS_SO" ] && [ -f "$OPENBLAS_SO" ]; then
            copy_lib "$OPENBLAS_SO"
            # Also copy with the .so.0 name that some systems expect
            OPENBLAS_BASENAME=$(basename "$OPENBLAS_SO")
            if [[ "$OPENBLAS_BASENAME" != *".so.0"* ]]; then
                # Create a symlink with .so.0 suffix
                ln -sf "$OPENBLAS_BASENAME" "${NATIVE_OUTPUT_DIR}/libopenblas.so.0" 2>/dev/null || true
            fi
        fi
    fi
    
    # Copy libgomp if needed
    if echo "$DEPS" | grep -q "libgomp"; then
        GOMP_SO=$(echo "$DEPS" | grep "libgomp" | awk '{print $3}' | head -1)
        if [ -n "$GOMP_SO" ] && [ -f "$GOMP_SO" ]; then
            copy_lib "$GOMP_SO"
        fi
    fi
    
    # Copy libgfortran if needed
    if echo "$DEPS" | grep -q "libgfortran"; then
        GFORTRAN_SO=$(echo "$DEPS" | grep "libgfortran" | awk '{print $3}' | head -1)
        if [ -n "$GFORTRAN_SO" ] && [ -f "$GFORTRAN_SO" ]; then
            copy_lib "$GFORTRAN_SO"
        fi
    fi
    
    # Copy libquadmath if needed
    if echo "$DEPS" | grep -q "libquadmath"; then
        QUADMATH_SO=$(echo "$DEPS" | grep "libquadmath" | awk '{print $3}' | head -1)
        if [ -n "$QUADMATH_SO" ] && [ -f "$QUADMATH_SO" ]; then
            copy_lib "$QUADMATH_SO"
        fi
    fi
fi

echo ""
echo "============================================================"
echo "  Build Complete!"
echo "============================================================"
echo ""
echo "Output directory: ${NATIVE_OUTPUT_DIR}"
echo ""
echo "Files:"
ls -lh "${NATIVE_OUTPUT_DIR}/"
echo ""

