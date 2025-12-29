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

# Find OpenMP (optional on some systems)
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

# Find FAISS
# FAISS can be installed via Homebrew: brew install faiss
# Or via conda: conda install -c pytorch faiss-cpu
# Or built from source: https://github.com/facebookresearch/faiss
find_package(faiss QUIET)
if(NOT faiss_FOUND)
    # Try to find FAISS using pkg-config
    find_package(PkgConfig)
    if(PkgConfig_FOUND)
        pkg_check_modules(FAISS faiss)
    endif()
    
    if(NOT FAISS_FOUND)
        # Manual search for FAISS - check build directory structure first
        # FAISS build directory has: build/faiss/libfaiss.so and headers in source dir
        find_path(FAISS_INCLUDE_DIR faiss/Index.h
            HINTS 
                $ENV{FAISS_HOME}
                $ENV{FAISS_HOME}/include
                $ENV{FAISS_BUILD_DIR}/../
                /root/faiss/faiss
                /usr/local/include
                /opt/homebrew/include
        )
        find_library(FAISS_LIBRARY faiss
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
        
        if(FAISS_INCLUDE_DIR AND FAISS_LIBRARY)
            set(FAISS_FOUND TRUE)
            set(FAISS_INCLUDE_DIRS ${FAISS_INCLUDE_DIR})
            set(FAISS_LIBRARIES ${FAISS_LIBRARY})
            message(STATUS "Found FAISS include dir: ${FAISS_INCLUDE_DIR}")
            message(STATUS "Found FAISS library: ${FAISS_LIBRARY}")
        else()
            message(FATAL_ERROR "FAISS not found. Please install FAISS:\n"
                "  brew install faiss (macOS)\n"
                "  conda install -c pytorch faiss-cpu\n"
                "Or set FAISS_HOME environment variable to the FAISS source/install directory.")
        endif()
    endif()
endif()

if(faiss_FOUND)
    # Using CMake config from installed FAISS
    set(FAISS_LIBRARIES faiss)
else()
    include_directories(${FAISS_INCLUDE_DIRS})
endif()

# Create the shared library
add_library(paimon_faiss_jni SHARED
    org_apache_paimon_faiss_jni_FaissJNI.cpp
)

# Link libraries
target_link_libraries(paimon_faiss_jni
    ${FAISS_LIBRARIES}
    ${JNI_LIBRARIES}
)

# Link OpenMP if found
if(OpenMP_CXX_FOUND)
    target_link_libraries(paimon_faiss_jni OpenMP::OpenMP_CXX)
elseif(APPLE AND LIBOMP_PREFIX)
    target_link_libraries(paimon_faiss_jni "${LIBOMP_PREFIX}/lib/libomp.dylib")
endif()

# Set output directory
set_target_properties(paimon_faiss_jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)

# Platform-specific settings
if(APPLE)
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".dylib"
    )
elseif(UNIX)
    set_target_properties(paimon_faiss_jni PROPERTIES
        SUFFIX ".so"
    )
endif()

