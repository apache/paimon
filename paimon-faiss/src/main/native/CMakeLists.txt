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

cmake_minimum_required(VERSION 3.10)
project(paimon_faiss_jni)

# Try C++17 first, fall back to C++14 if not available
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-std=c++17" COMPILER_SUPPORTS_CXX17)
check_cxx_compiler_flag("-std=c++14" COMPILER_SUPPORTS_CXX14)
check_cxx_compiler_flag("-std=c++11" COMPILER_SUPPORTS_CXX11)

if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_STANDARD 17)
    message(STATUS "Using C++17")
elseif(COMPILER_SUPPORTS_CXX14)
    set(CMAKE_CXX_STANDARD 14)
    message(STATUS "Using C++14")
elseif(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_STANDARD 11)
    message(STATUS "Using C++11")
else()
    message(FATAL_ERROR "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Option to build fat library with all dependencies bundled
option(FAT_LIBRARY "Build fat library with all dependencies statically linked" ON)

# macOS specific: Find OpenMP from Homebrew
if(APPLE)
    # Find libomp installed via Homebrew
    execute_process(
        COMMAND brew --prefix libomp
        OUTPUT_VARIABLE LIBOMP_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    
    if(LIBOMP_PREFIX)
        set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
        set(OpenMP_C_LIB_NAMES "omp")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "${LIBOMP_PREFIX}/lib/libomp.dylib")
        
        include_directories(${LIBOMP_PREFIX}/include)
        link_directories(${LIBOMP_PREFIX}/lib)
    else()
        message(WARNING "libomp not found. Install with: brew install libomp")
    endif()
endif()

# Find OpenMP
find_package(OpenMP QUIET)
if(OpenMP_CXX_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    message(STATUS "OpenMP found")
else()
    message(STATUS "OpenMP not found - building without OpenMP support")
endif()

# Find JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Find FAISS include directory
find_path(FAISS_INCLUDE_DIR faiss/Index.h
    HINTS 
        $ENV{FAISS_HOME}
        $ENV{FAISS_HOME}/include
        $ENV{FAISS_BUILD_DIR}/../
        /root/faiss/faiss
        /usr/local/include
        /opt/homebrew/include
)

if(FAISS_INCLUDE_DIR)
    include_directories(${FAISS_INCLUDE_DIR})
    message(STATUS "Found FAISS include dir: ${FAISS_INCLUDE_DIR}")
else()
    message(FATAL_ERROR "FAISS headers not found. Set FAISS_HOME environment variable.")
endif()

# Collect all static libraries for fat library build
set(STATIC_LIBS_TO_BUNDLE "")

if(FAT_LIBRARY AND NOT APPLE)
    message(STATUS "")
    message(STATUS "========== Building FAT library with all dependencies ==========")
    message(STATUS "")
    
    # Find static FAISS library
    find_library(FAISS_STATIC_LIB
        NAMES libfaiss.a faiss.a
        HINTS
            $ENV{FAISS_BUILD_DIR}/faiss
            $ENV{FAISS_HOME}/build/faiss
            $ENV{FAISS_HOME}/lib
            /root/faiss/faiss/build/faiss
            ${HOME}/faiss/build/faiss
            /usr/local/lib
            /usr/lib
            /usr/lib64
            /usr/lib/x86_64-linux-gnu
            /usr/lib/aarch64-linux-gnu
        NO_DEFAULT_PATH
    )
    if(NOT FAISS_STATIC_LIB)
        find_library(FAISS_STATIC_LIB NAMES libfaiss.a)
    endif()
    
    if(FAISS_STATIC_LIB)
        message(STATUS "Found static FAISS: ${FAISS_STATIC_LIB}")
        list(APPEND STATIC_LIBS_TO_BUNDLE ${FAISS_STATIC_LIB})
    else()
        message(FATAL_ERROR "Static FAISS library (libfaiss.a) not found!\n"
            "Build FAISS from source with:\n"
            "  git clone https://github.com/facebookresearch/faiss.git\n"
            "  cd faiss\n"
            "  cmake -B build -DBUILD_SHARED_LIBS=OFF -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF .\n"
            "  cmake --build build -j$(nproc)\n"
            "Then set: export FAISS_HOME=$(pwd) FAISS_BUILD_DIR=$(pwd)/build")
    endif()
    
    # Common library search paths for both x86_64 and aarch64
    set(LIB_SEARCH_PATHS
        /usr/local/lib
        /usr/lib
        /usr/lib64
        /usr/lib/x86_64-linux-gnu
        /usr/lib/aarch64-linux-gnu
        /opt/OpenBLAS/lib
    )
    
    # GCC library search paths
    set(GCC_SEARCH_PATHS
        /usr/lib/gcc/x86_64-linux-gnu
        /usr/lib/gcc/aarch64-linux-gnu
        /usr/lib64/gcc/x86_64-linux-gnu
        /usr/lib64/gcc/aarch64-linux-gnu
        /usr/lib/gcc
    )
    
    # Find static OpenBLAS
    find_library(OPENBLAS_STATIC_LIB
        NAMES libopenblas.a openblas.a
        HINTS ${LIB_SEARCH_PATHS}
        NO_DEFAULT_PATH
    )
    if(NOT OPENBLAS_STATIC_LIB)
        find_library(OPENBLAS_STATIC_LIB NAMES libopenblas.a)
    endif()
    
    if(OPENBLAS_STATIC_LIB)
        message(STATUS "Found static OpenBLAS: ${OPENBLAS_STATIC_LIB}")
        list(APPEND STATIC_LIBS_TO_BUNDLE ${OPENBLAS_STATIC_LIB})
    else()
        message(STATUS "Static OpenBLAS not found, will try dynamic linking")
    endif()
    
    # Find static LAPACK (optional)
    find_library(LAPACK_STATIC_LIB
        NAMES liblapack.a lapack.a
        HINTS ${LIB_SEARCH_PATHS}
        NO_DEFAULT_PATH
    )
    if(NOT LAPACK_STATIC_LIB)
        find_library(LAPACK_STATIC_LIB NAMES liblapack.a)
    endif()
    
    if(LAPACK_STATIC_LIB)
        message(STATUS "Found static LAPACK: ${LAPACK_STATIC_LIB}")
        list(APPEND STATIC_LIBS_TO_BUNDLE ${LAPACK_STATIC_LIB})
    endif()
    
    # Find static BLAS (sometimes needed separately)
    find_library(BLAS_STATIC_LIB
        NAMES libblas.a blas.a
        HINTS ${LIB_SEARCH_PATHS}
    )
    if(BLAS_STATIC_LIB)
        message(STATUS "Found static BLAS: ${BLAS_STATIC_LIB}")
        list(APPEND STATIC_LIBS_TO_BUNDLE ${BLAS_STATIC_LIB})
    endif()
    
    # Note: We do NOT statically link GCC runtime libraries (libgfortran, libgomp, libquadmath)
    # because they are often not compiled with -fPIC on many systems (especially aarch64),
    # which causes linker errors when building a shared library.
    # These libraries are widely available as shared libraries on Linux systems.
    message(STATUS "GCC runtime libraries (gfortran, gomp, quadmath) will be dynamically linked")
    set(GFORTRAN_STATIC_LIB "" CACHE INTERNAL "Not using static gfortran - use dynamic linking")
    set(GOMP_STATIC_LIB "" CACHE INTERNAL "Not using static GOMP - use dynamic linking")
    set(QUADMATH_STATIC_LIB "" CACHE INTERNAL "Not using static quadmath - use dynamic linking")
    
    message(STATUS "")
    message(STATUS "Static libraries to bundle: ${STATIC_LIBS_TO_BUNDLE}")
    message(STATUS "")
    
    set(FAT_BUILD TRUE)
else()
    # Find shared FAISS library for dynamic linking
    find_library(FAISS_SHARED_LIB
        NAMES faiss libfaiss.so
        HINTS
            $ENV{FAISS_BUILD_DIR}/faiss
            $ENV{FAISS_HOME}/build/faiss
            $ENV{FAISS_HOME}/lib
            /root/faiss/faiss/build/faiss
            /usr/local/lib
            /usr/lib
            /usr/lib64
            /opt/homebrew/lib
    )
    
    if(FAISS_SHARED_LIB)
        message(STATUS "Found shared FAISS: ${FAISS_SHARED_LIB}")
    else()
        # Try CMake config
        find_package(faiss QUIET)
        if(faiss_FOUND)
            set(FAISS_SHARED_LIB faiss)
            message(STATUS "Found FAISS via CMake config")
        else()
            message(FATAL_ERROR "FAISS library not found. Install or build FAISS first.")
        endif()
    endif()
    
    set(FAT_BUILD FALSE)
endif()

# Create the shared library
add_library(paimon_faiss_jni SHARED
    org_apache_paimon_faiss_jni_FaissJNI.cpp
)

# Link libraries
if(FAT_BUILD)
    message(STATUS "Linking all dependencies into fat library...")
    
    # For proper fat library linking, we need to:
    # 1. Use -Wl,--whole-archive to include ALL symbols from static libs
    # 2. Use -Wl,--no-whole-archive after the static libs
    # 3. Link system libraries normally
    
    # Build the whole-archive link line
    set(WHOLE_ARCHIVE_LIBS "")
    foreach(static_lib ${STATIC_LIBS_TO_BUNDLE})
        list(APPEND WHOLE_ARCHIVE_LIBS ${static_lib})
    endforeach()
    
    # Use generator expressions for proper whole-archive linking
    target_link_libraries(paimon_faiss_jni PRIVATE
        -Wl,--whole-archive
        ${WHOLE_ARCHIVE_LIBS}
        -Wl,--no-whole-archive
        ${JNI_LIBRARIES}
        pthread
        m
        dl
    )
    
    # If static OpenBLAS wasn't found, link dynamically
    if(NOT OPENBLAS_STATIC_LIB)
        target_link_libraries(paimon_faiss_jni PRIVATE openblas)
    endif()
    
    # Always link gomp dynamically - static libgomp.a often lacks -fPIC
    if(OpenMP_CXX_FOUND)
        target_link_libraries(paimon_faiss_jni PRIVATE gomp)
    endif()
    
    # Always link gfortran dynamically if LAPACK/BLAS is used
    # Static libgfortran.a often lacks -fPIC on aarch64
    if(LAPACK_STATIC_LIB OR BLAS_STATIC_LIB OR OPENBLAS_STATIC_LIB)
        target_link_libraries(paimon_faiss_jni PRIVATE gfortran)
    endif()
    
    # Set RPATH to $ORIGIN for any remaining shared deps
    set_target_properties(paimon_faiss_jni PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$ORIGIN"
    )
    
    # Hide internal symbols to reduce size and avoid symbol conflicts
    set_target_properties(paimon_faiss_jni PROPERTIES
        C_VISIBILITY_PRESET hidden
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
    
    # Strip unnecessary symbols to reduce size
    add_custom_command(TARGET paimon_faiss_jni POST_BUILD
        COMMAND ${CMAKE_STRIP} --strip-unneeded $<TARGET_FILE:paimon_faiss_jni> 2>/dev/null || true
        COMMENT "Stripping unnecessary symbols from fat library"
    )
else()
    # Dynamic linking
    target_link_libraries(paimon_faiss_jni
        ${FAISS_SHARED_LIB}
        ${JNI_LIBRARIES}
    )
    
    if(OpenMP_CXX_FOUND)
        target_link_libraries(paimon_faiss_jni OpenMP::OpenMP_CXX)
    elseif(APPLE AND LIBOMP_PREFIX)
        target_link_libraries(paimon_faiss_jni "${LIBOMP_PREFIX}/lib/libomp.dylib")
    endif()
endif()

# Set output directory
set_target_properties(paimon_faiss_jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)

# Platform-specific settings
if(APPLE)
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".dylib"
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "@loader_path;/opt/homebrew/lib;/usr/local/lib"
    )
elseif(UNIX)
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".so"
    )
endif()

