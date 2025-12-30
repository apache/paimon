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

#
# This script builds native libraries for different platforms.
# 
# Supported platforms:
#   - linux/amd64
#   - linux/aarch64
#   - darwin/amd64
#   - darwin/aarch64
#
# Usage:
#   ./build-all-platforms.sh                    # Build for current platform
#   ./build-all-platforms.sh --platform linux/amd64  # Build for specific platform (via Docker)
#   ./build-all-platforms.sh --all              # Build for all platforms (requires Docker)
#   ./build-all-platforms.sh --list             # List supported platforms
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESOURCES_DIR="$PROJECT_DIR/src/main/resources"

# Supported platforms (Linux and macOS only)
PLATFORMS=(
    "linux/amd64"
    "linux/aarch64"
    "darwin/amd64"
    "darwin/aarch64"
)

# Get Docker image for a platform
get_docker_image() {
    local platform="$1"
    case "$platform" in
        "linux/amd64")
            echo "dockcross/linux-x64"
            ;;
        "linux/aarch64")
            echo "dockcross/linux-arm64"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --platform PLATFORM   Build for specific platform (e.g., linux/amd64)"
    echo "  --current             Build for current platform only (default)"
    echo "  --all                 Build for all supported platforms (requires Docker)"
    echo "  --list                List all supported platforms"
    echo "  --clean               Clean build directory before building"
    echo "  --opt-level LEVEL     Optimization level: generic, avx2, avx512 (default: generic)"
    echo "  --help                Show this help message"
    echo ""
    echo "Supported platforms:"
    for platform in "${PLATFORMS[@]}"; do
        echo "  - $platform"
    done
    echo ""
    echo "Examples:"
    echo "  $0                           # Build for current platform"
    echo "  $0 --platform linux/amd64    # Build for Linux x86_64 (via Docker)"
    echo "  $0 --all                     # Build for all platforms"
}

list_platforms() {
    echo "Supported platforms:"
    echo ""
    for platform in "${PLATFORMS[@]}"; do
        os=$(echo "$platform" | cut -d'/' -f1)
        arch=$(echo "$platform" | cut -d'/' -f2)
        
        case "$os" in
            linux)
                lib_ext="so"
                ;;
            darwin)
                lib_ext="dylib"
                ;;
        esac
        
        echo "  $platform"
        echo "    Library: libpaimon_faiss_jni.$lib_ext"
        echo "    Output:  src/main/resources/$os/$arch/"
        echo ""
    done
}

get_current_platform() {
    local os arch
    
    case "$(uname -s)" in
        Linux)
            os="linux"
            ;;
        Darwin)
            os="darwin"
            ;;
        *)
            print_error "Unsupported OS: $(uname -s). Only Linux and macOS are supported."
            exit 1
            ;;
    esac
    
    case "$(uname -m)" in
        x86_64|amd64)
            arch="amd64"
            ;;
        aarch64|arm64)
            arch="aarch64"
            ;;
        *)
            print_error "Unsupported architecture: $(uname -m)"
            exit 1
            ;;
    esac
    
    echo "$os/$arch"
}

build_current_platform() {
    local opt_level="$1"
    local clean="$2"
    
    print_header "Building for current platform: $(get_current_platform)"
    
    local args=()
    if [ -n "$opt_level" ]; then
        args+=(--opt-level "$opt_level")
    fi
    if [ "$clean" = true ]; then
        args+=(--clean)
    fi
    
    "$SCRIPT_DIR/build-native.sh" "${args[@]}"
}

build_with_docker() {
    local platform="$1"
    local opt_level="$2"
    local clean="$3"
    
    local docker_image
    docker_image=$(get_docker_image "$platform")
    
    if [ -z "$docker_image" ]; then
        print_error "No Docker image available for platform: $platform"
        print_warning "This platform must be built natively on the target OS."
        return 1
    fi
    
    print_header "Building for $platform using Docker"
    echo "Using image: $docker_image"
    echo ""
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Pull the image if needed
    echo "Pulling Docker image..."
    docker pull "$docker_image"
    
    # Create build script for container
    local build_script="/tmp/build-in-docker.sh"
    cat > "$build_script" << 'DOCKER_SCRIPT'
#!/bin/bash
set -e
cd /work
mkdir -p build/native
cd build/native
cmake -DCMAKE_BUILD_TYPE=Release ../../src/main/native
cmake --build . --config Release -j $(nproc)
DOCKER_SCRIPT
    chmod +x "$build_script"
    
    # Run build in container
    docker run --rm \
        -v "$PROJECT_DIR:/work" \
        -v "$build_script:/build.sh:ro" \
        "$docker_image" \
        bash /build.sh
    
    rm -f "$build_script"
    
    print_success "Build completed for $platform"
}

build_platform() {
    local platform="$1"
    local opt_level="$2"
    local clean="$3"
    local current_platform
    
    current_platform=$(get_current_platform)
    
    if [ "$platform" = "$current_platform" ]; then
        build_current_platform "$opt_level" "$clean"
    else
        build_with_docker "$platform" "$opt_level" "$clean"
    fi
}

build_all_platforms() {
    local opt_level="$1"
    local clean="$2"
    local current_platform
    local failed_platforms=()
    
    current_platform=$(get_current_platform)
    
    print_header "Building for All Platforms"
    echo ""
    echo "Current platform: $current_platform"
    echo ""
    
    for platform in "${PLATFORMS[@]}"; do
        echo ""
        echo "----------------------------------------"
        
        if [ "$platform" = "$current_platform" ]; then
            build_current_platform "$opt_level" "$clean"
        else
            if ! build_with_docker "$platform" "$opt_level" "$clean"; then
                failed_platforms+=("$platform")
            fi
        fi
    done
    
    echo ""
    print_header "Build Summary"
    echo ""
    
    echo "Native libraries in $RESOURCES_DIR:"
    find "$RESOURCES_DIR" -type f \( -name "*.so" -o -name "*.dylib" \) 2>/dev/null | while read -r lib; do
        echo "  - $lib"
    done
    
    if [ ${#failed_platforms[@]} -gt 0 ]; then
        echo ""
        print_warning "Some platforms could not be built:"
        for p in "${failed_platforms[@]}"; do
            echo "  - $p (build natively on target platform)"
        done
    fi
}

show_native_libs() {
    echo ""
    echo "Native libraries in resources directory:"
    echo ""
    
    for platform in "${PLATFORMS[@]}"; do
        os=$(echo "$platform" | cut -d'/' -f1)
        arch=$(echo "$platform" | cut -d'/' -f2)
        dir="$RESOURCES_DIR/$os/$arch"
        
        if [ -d "$dir" ]; then
            libs=$(find "$dir" -type f \( -name "*.so" -o -name "*.dylib" \) 2>/dev/null)
            if [ -n "$libs" ]; then
                print_success "$platform:"
                echo "$libs" | while read -r lib; do
                    size=$(ls -lh "$lib" | awk '{print $5}')
                    echo "    $(basename "$lib") ($size)"
                done
            else
                print_warning "$platform: (not built)"
            fi
        else
            print_warning "$platform: (directory not found)"
        fi
    done
}

# Parse arguments
PLATFORM=""
BUILD_ALL=false
BUILD_CURRENT=false
LIST_ONLY=false
CLEAN=false
OPT_LEVEL="generic"

while [[ $# -gt 0 ]]; do
    case $1 in
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --current)
            BUILD_CURRENT=true
            shift
            ;;
        --all)
            BUILD_ALL=true
            shift
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --opt-level)
            OPT_LEVEL="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

# Execute based on options
if [ "$LIST_ONLY" = true ]; then
    list_platforms
    show_native_libs
    exit 0
fi

if [ "$BUILD_ALL" = true ]; then
    build_all_platforms "$OPT_LEVEL" "$CLEAN"
elif [ -n "$PLATFORM" ]; then
    # Validate platform
    valid=false
    for p in "${PLATFORMS[@]}"; do
        if [ "$p" = "$PLATFORM" ]; then
            valid=true
            break
        fi
    done
    
    if [ "$valid" = false ]; then
        print_error "Invalid platform: $PLATFORM"
        echo ""
        list_platforms
        exit 1
    fi
    
    build_platform "$PLATFORM" "$OPT_LEVEL" "$CLEAN"
else
    # Default: build for current platform
    build_current_platform "$OPT_LEVEL" "$CLEAN"
fi

echo ""
show_native_libs
echo ""
echo "To package the JAR with all native libraries, run:"
echo "  mvn package"
