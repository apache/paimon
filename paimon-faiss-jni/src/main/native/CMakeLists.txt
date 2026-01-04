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

cmake_minimum_required(VERSION 3.30.1)
project(paimon_faiss_jni VERSION 0.1.0 LANGUAGES CXX)

# Check GCC version (must be >= 9.3.0)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.3.0")
        message(FATAL_ERROR "GCC version must be >= 9.3.0. Found: ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
    message(STATUS "Using GCC ${CMAKE_CXX_COMPILER_VERSION}")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Options
option(FAISS_ENABLE_GPU "Build with GPU support" OFF)
option(FAISS_OPT_LEVEL "Optimization level (generic, avx2, avx512)" "generic")
option(BUILD_FAT_LIB "Build fat library with all dependencies statically linked" ON)

# Find JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Find OpenMP (with special handling for macOS)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS requires special handling for OpenMP
    # First try to find libomp from Homebrew
    execute_process(
        COMMAND brew --prefix libomp
        OUTPUT_VARIABLE HOMEBREW_LIBOMP_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    
    if(HOMEBREW_LIBOMP_PREFIX)
        message(STATUS "Found Homebrew libomp: ${HOMEBREW_LIBOMP_PREFIX}")
        set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I${HOMEBREW_LIBOMP_PREFIX}/include")
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${HOMEBREW_LIBOMP_PREFIX}/include")
        set(OpenMP_C_LIB_NAMES "omp")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "${HOMEBREW_LIBOMP_PREFIX}/lib/libomp.dylib")
        
        # Create imported target manually
        if(NOT TARGET OpenMP::OpenMP_CXX)
            add_library(OpenMP::OpenMP_CXX SHARED IMPORTED)
            set_target_properties(OpenMP::OpenMP_CXX PROPERTIES
                IMPORTED_LOCATION "${HOMEBREW_LIBOMP_PREFIX}/lib/libomp.dylib"
                INTERFACE_INCLUDE_DIRECTORIES "${HOMEBREW_LIBOMP_PREFIX}/include"
                INTERFACE_COMPILE_OPTIONS "-Xpreprocessor;-fopenmp"
            )
        endif()
        set(OpenMP_FOUND TRUE)
    else()
        message(WARNING "libomp not found via Homebrew. Trying standard OpenMP detection...")
        find_package(OpenMP)
    endif()
else()
    find_package(OpenMP REQUIRED)
endif()

if(NOT OpenMP_FOUND AND NOT TARGET OpenMP::OpenMP_CXX)
    message(WARNING "OpenMP not found. Building without OpenMP support.")
    message(WARNING "On macOS, install libomp: brew install libomp")
endif()

# Find Faiss
# For fat lib, prefer static libraries
if(BUILD_FAT_LIB)
    message(STATUS "Building fat library - preferring static libraries")
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".so" ".dylib")
    set(FAISS_STATIC_PREFERRED TRUE)
else()
    set(FAISS_STATIC_PREFERRED FALSE)
endif()

# First try to find Faiss via CMake config
find_package(faiss CONFIG QUIET)

if(NOT faiss_FOUND)
    # Try pkg-config
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(FAISS QUIET faiss)
    endif()
    
    if(NOT FAISS_FOUND)
        # Manual search - look in common locations
        find_path(FAISS_INCLUDE_DIR
            NAMES faiss/Index.h
            PATHS
                /usr/local/include
                /usr/include
                ${FAISS_ROOT}/include
                $ENV{FAISS_ROOT}/include
        )
        
        # For fat lib, try to find static library first
        if(BUILD_FAT_LIB)
            find_library(FAISS_LIBRARY_STATIC
                NAMES libfaiss.a faiss_static
                PATHS
                    /usr/local/lib
                    /usr/lib
                    /usr/local/lib64
                    /usr/lib64
                    ${FAISS_ROOT}/lib
                    ${FAISS_ROOT}/lib64
                    $ENV{FAISS_ROOT}/lib
                    $ENV{FAISS_ROOT}/lib64
            )
            if(FAISS_LIBRARY_STATIC)
                set(FAISS_LIBRARY ${FAISS_LIBRARY_STATIC})
                message(STATUS "Found Faiss static library: ${FAISS_LIBRARY}")
            endif()
        endif()
        
        # If static not found or not building fat lib, find any library
        if(NOT FAISS_LIBRARY)
            find_library(FAISS_LIBRARY
                NAMES faiss
                PATHS
                    /usr/local/lib
                    /usr/lib
                    /usr/local/lib64
                    /usr/lib64
                    ${FAISS_ROOT}/lib
                    ${FAISS_ROOT}/lib64
                    $ENV{FAISS_ROOT}/lib
                    $ENV{FAISS_ROOT}/lib64
            )
        endif()
        
        if(FAISS_INCLUDE_DIR AND FAISS_LIBRARY)
            set(FAISS_FOUND TRUE)
            set(FAISS_INCLUDE_DIRS ${FAISS_INCLUDE_DIR})
            set(FAISS_LIBRARIES ${FAISS_LIBRARY})
            message(STATUS "Found Faiss: ${FAISS_LIBRARY}")
        else()
            message(FATAL_ERROR "Faiss not found. Please install Faiss or set FAISS_ROOT environment variable.")
        endif()
    endif()
endif()

# Find BLAS/LAPACK for static linking (Faiss depends on them)
if(BUILD_FAT_LIB)
    # Save original suffixes
    set(_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
    
    # Force static library search only
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    
    # Try to find OpenBLAS static library
    find_library(OPENBLAS_STATIC_LIBRARY
        NAMES openblas openblas_static
        PATHS
            /usr/local/lib
            /usr/lib
            /usr/local/lib64
            /usr/lib64
            /usr/lib/x86_64-linux-gnu
            /usr/lib/aarch64-linux-gnu
            ${OPENBLAS_ROOT}/lib
            $ENV{OPENBLAS_ROOT}/lib
        NO_DEFAULT_PATH
    )
    
    # Also try default paths
    if(NOT OPENBLAS_STATIC_LIBRARY)
        find_library(OPENBLAS_STATIC_LIBRARY
            NAMES openblas openblas_static
        )
    endif()
    
    if(OPENBLAS_STATIC_LIBRARY AND OPENBLAS_STATIC_LIBRARY MATCHES "\\.a$")
        message(STATUS "Found OpenBLAS static library: ${OPENBLAS_STATIC_LIBRARY}")
        set(OPENBLAS_USE_STATIC TRUE)
        list(APPEND FAISS_STATIC_LIBS ${OPENBLAS_STATIC_LIBRARY})
    else()
        message(STATUS "OpenBLAS static library not found, trying shared library")
        set(OPENBLAS_USE_STATIC FALSE)
        
        # Restore suffixes and find shared library
        set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
        find_library(OPENBLAS_SHARED_LIBRARY
            NAMES openblas
            PATHS
                /usr/local/lib
                /usr/lib
                /usr/local/lib64
                /usr/lib64
                /usr/lib/x86_64-linux-gnu
                /usr/lib/aarch64-linux-gnu
                ${OPENBLAS_ROOT}/lib
                $ENV{OPENBLAS_ROOT}/lib
        )
        if(OPENBLAS_SHARED_LIBRARY)
            message(STATUS "Found OpenBLAS shared library: ${OPENBLAS_SHARED_LIBRARY}")
            list(APPEND FAISS_EXTRA_LIBS ${OPENBLAS_SHARED_LIBRARY})
            # Mark that we need to bundle this library
            set(BUNDLE_OPENBLAS TRUE)
            set(BUNDLE_OPENBLAS_PATH ${OPENBLAS_SHARED_LIBRARY})
        else()
            # Try to find any BLAS
            find_package(BLAS QUIET)
            if(BLAS_FOUND)
                list(APPEND FAISS_EXTRA_LIBS ${BLAS_LIBRARIES})
                message(STATUS "Found BLAS: ${BLAS_LIBRARIES}")
            endif()
        endif()
    endif()
    
    # Restore suffixes for static search
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    
    # Find LAPACK static library
    find_library(LAPACK_STATIC_LIBRARY
        NAMES lapack
        PATHS
            /usr/local/lib
            /usr/lib
            /usr/local/lib64
            /usr/lib64
            /usr/lib/x86_64-linux-gnu
            /usr/lib/aarch64-linux-gnu
    )
    if(LAPACK_STATIC_LIBRARY AND LAPACK_STATIC_LIBRARY MATCHES "\\.a$")
        message(STATUS "Found LAPACK static library: ${LAPACK_STATIC_LIBRARY}")
        list(APPEND FAISS_STATIC_LIBS ${LAPACK_STATIC_LIBRARY})
    endif()
    
    # Find gfortran static library (needed by OpenBLAS)
    find_library(GFORTRAN_STATIC_LIBRARY
        NAMES gfortran
    )
    if(GFORTRAN_STATIC_LIBRARY AND GFORTRAN_STATIC_LIBRARY MATCHES "\\.a$")
        message(STATUS "Found gfortran static library: ${GFORTRAN_STATIC_LIBRARY}")
        list(APPEND FAISS_STATIC_LIBS ${GFORTRAN_STATIC_LIBRARY})
    endif()
    
    # Restore original suffixes
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
    
    # On Linux, we may need pthread, dl, and m (these are typically dynamically linked)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        list(APPEND FAISS_EXTRA_LIBS pthread dl m)
        
        # Try to find gfortran shared if static not found
        if(NOT GFORTRAN_STATIC_LIBRARY)
            find_library(GFORTRAN_LIBRARY gfortran)
            if(GFORTRAN_LIBRARY)
                list(APPEND FAISS_EXTRA_LIBS ${GFORTRAN_LIBRARY})
            endif()
        endif()
    endif()
endif()

# Platform detection - using {os}/{arch} directory structure
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(PLATFORM_OS "linux")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
        set(PLATFORM_ARCH "amd64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
        set(PLATFORM_ARCH "aarch64")
    else()
        message(FATAL_ERROR "Unsupported Linux architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(PLATFORM_OS "darwin")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64")
        set(PLATFORM_ARCH "amd64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
        set(PLATFORM_ARCH "aarch64")
    else()
        message(FATAL_ERROR "Unsupported macOS architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
else()
    message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}. Only Linux and macOS are supported.")
endif()

set(PLATFORM_DIR "${PLATFORM_OS}/${PLATFORM_ARCH}")
message(STATUS "Building for platform: ${PLATFORM_DIR}")

# Build the JNI library
add_library(paimon_faiss_jni SHARED
    paimon_faiss_jni.cpp
)

# Include directories
if(TARGET faiss)
    target_link_libraries(paimon_faiss_jni PRIVATE faiss)
else()
    target_include_directories(paimon_faiss_jni PRIVATE ${FAISS_INCLUDE_DIRS})
    target_link_libraries(paimon_faiss_jni PRIVATE ${FAISS_LIBRARIES})
endif()

# Link extra libraries for fat lib (BLAS, LAPACK, etc.)
if(BUILD_FAT_LIB)
    # Link static libraries with --whole-archive to embed all symbols
    if(FAISS_STATIC_LIBS AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(STATUS "Linking static libraries with --whole-archive: ${FAISS_STATIC_LIBS}")
        target_link_options(paimon_faiss_jni PRIVATE
            "-Wl,--whole-archive"
        )
        target_link_libraries(paimon_faiss_jni PRIVATE ${FAISS_STATIC_LIBS})
        target_link_options(paimon_faiss_jni PRIVATE
            "-Wl,--no-whole-archive"
        )
    elseif(FAISS_STATIC_LIBS)
        # macOS doesn't use --whole-archive, use -force_load instead
        foreach(static_lib ${FAISS_STATIC_LIBS})
            target_link_options(paimon_faiss_jni PRIVATE "-Wl,-force_load,${static_lib}")
        endforeach()
        message(STATUS "Linking static libraries with -force_load: ${FAISS_STATIC_LIBS}")
    endif()
    
    # Link remaining shared libraries
    if(FAISS_EXTRA_LIBS)
        target_link_libraries(paimon_faiss_jni PRIVATE ${FAISS_EXTRA_LIBS})
        message(STATUS "Linking extra libraries: ${FAISS_EXTRA_LIBS}")
    endif()
endif()

# Link OpenMP - always use dynamic linking for OpenMP (static libgomp.a often lacks -fPIC)
if(TARGET OpenMP::OpenMP_CXX)
    target_link_libraries(paimon_faiss_jni PRIVATE OpenMP::OpenMP_CXX)
    message(STATUS "Linking OpenMP via imported target")
elseif(OpenMP_FOUND)
    target_compile_options(paimon_faiss_jni PRIVATE ${OpenMP_CXX_FLAGS})
    # Link against the shared gomp library
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        find_library(GOMP_SHARED_LIBRARY NAMES gomp PATHS /usr/lib /usr/lib64 /usr/lib/x86_64-linux-gnu)
        if(GOMP_SHARED_LIBRARY)
            target_link_libraries(paimon_faiss_jni PRIVATE ${GOMP_SHARED_LIBRARY})
            message(STATUS "Linking OpenMP shared library: ${GOMP_SHARED_LIBRARY}")
        else()
            target_link_libraries(paimon_faiss_jni PRIVATE gomp)
            message(STATUS "Linking OpenMP: gomp")
        endif()
    else()
        target_link_libraries(paimon_faiss_jni PRIVATE ${OpenMP_CXX_FLAGS})
    endif()
endif()

# Platform-specific settings
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS specific settings
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".dylib"
        INSTALL_NAME_DIR "@rpath"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
    
    # Link against libc++
    target_link_libraries(paimon_faiss_jni PRIVATE c++)
    
    # For fat lib on macOS, embed OpenMP library path
    if(BUILD_FAT_LIB AND HOMEBREW_LIBOMP_PREFIX)
        target_link_options(paimon_faiss_jni PRIVATE 
            "-Wl,-rpath,@loader_path"
            "-Wl,-rpath,${HOMEBREW_LIBOMP_PREFIX}/lib"
        )
    endif()
    
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Linux specific settings
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".so"
    )
    
    if(BUILD_FAT_LIB)
        # For fat lib, use static libstdc++ and libgcc
        target_link_options(paimon_faiss_jni PRIVATE
            "-static-libstdc++"
            "-static-libgcc"
            "-Wl,--exclude-libs,ALL"
        )
        message(STATUS "Using static libstdc++ and libgcc for fat lib")
    else()
        target_link_libraries(paimon_faiss_jni PRIVATE stdc++)
    endif()
    
endif()

# Set output directory - output to src/main/resources/{os}/{arch}/
set(OUTPUT_DIR "${CMAKE_SOURCE_DIR}/../resources/${PLATFORM_DIR}")
set_target_properties(paimon_faiss_jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${OUTPUT_DIR}
    RUNTIME_OUTPUT_DIRECTORY ${OUTPUT_DIR}
)

# Optimization level
if(FAISS_OPT_LEVEL STREQUAL "avx2")
    target_compile_options(paimon_faiss_jni PRIVATE -mavx2 -mfma)
    message(STATUS "Building with AVX2 optimizations")
elseif(FAISS_OPT_LEVEL STREQUAL "avx512")
    target_compile_options(paimon_faiss_jni PRIVATE -mavx512f -mavx512dq -mavx512bw -mavx512vl)
    message(STATUS "Building with AVX-512 optimizations")
else()
    message(STATUS "Building with generic optimizations")
endif()

# Copy bundled shared libraries to output directory and set rpath
if(BUILD_FAT_LIB AND BUNDLE_OPENBLAS AND BUNDLE_OPENBLAS_PATH)
    message(STATUS "Will bundle OpenBLAS shared library: ${BUNDLE_OPENBLAS_PATH}")
    
    # Get the actual library file (resolve symlinks)
    get_filename_component(OPENBLAS_REALPATH ${BUNDLE_OPENBLAS_PATH} REALPATH)
    get_filename_component(OPENBLAS_FILENAME ${OPENBLAS_REALPATH} NAME)
    
    # Copy OpenBLAS to output directory after build
    add_custom_command(TARGET paimon_faiss_jni POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${OPENBLAS_REALPATH}
            ${OUTPUT_DIR}/libopenblas.so.0
        COMMENT "Bundling OpenBLAS shared library"
    )
    
    # Set rpath to look in the same directory as the library
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        target_link_options(paimon_faiss_jni PRIVATE
            "-Wl,-rpath,$ORIGIN"
        )
        # Also patch the library after build to use the bundled libopenblas
        add_custom_command(TARGET paimon_faiss_jni POST_BUILD
            COMMAND patchelf --set-rpath "$$ORIGIN" ${OUTPUT_DIR}/libpaimon_faiss_jni.so || true
            COMMENT "Setting rpath to $ORIGIN"
        )
    endif()
endif()

# Install target
install(TARGETS paimon_faiss_jni
    LIBRARY DESTINATION ${PLATFORM_DIR}
    RUNTIME DESTINATION ${PLATFORM_DIR}
)

