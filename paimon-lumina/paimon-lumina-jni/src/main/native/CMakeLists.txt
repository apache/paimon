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

cmake_minimum_required(VERSION 3.14)
project(paimon_lumina_jni VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ==================== Find JNI ====================
if(JAVA_HOME AND NOT DEFINED ENV{JAVA_HOME})
    set(ENV{JAVA_HOME} "${JAVA_HOME}")
endif()
if(DEFINED ENV{JAVA_HOME})
    set(JAVA_AWT_LIBRARY NotNeeded)
    set(JAVA_AWT_INCLUDE_PATH "$ENV{JAVA_HOME}/include")
endif()
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# ==================== Find Lumina ====================
if(NOT LUMINA_ROOT AND DEFINED ENV{LUMINA_ROOT})
    set(LUMINA_ROOT $ENV{LUMINA_ROOT})
endif()

find_path(LUMINA_INCLUDE_DIR
    NAMES lumina/api/LuminaBuilder.h
    PATHS
        ${LUMINA_ROOT}/include
        /usr/local/include
        /usr/include
)

if(NOT LUMINA_INCLUDE_DIR)
    message(FATAL_ERROR "Lumina headers not found. Set LUMINA_ROOT.")
endif()
message(STATUS "Found Lumina headers: ${LUMINA_INCLUDE_DIR}")

# ==================== Locate Lumina library ====================
# Priority: LUMINA_LIBRARY (exact path) > liblumina.a (static) > liblumina.so (shared)

if(LUMINA_LIBRARY)
    # User specified exact library path via -DLUMINA_LIBRARY=...
    if(NOT EXISTS "${LUMINA_LIBRARY}")
        message(FATAL_ERROR "LUMINA_LIBRARY does not exist: ${LUMINA_LIBRARY}")
    endif()
    if(LUMINA_LIBRARY MATCHES "\\.a$")
        set(LUMINA_IS_STATIC TRUE)
    else()
        set(LUMINA_IS_STATIC FALSE)
    endif()
    message(STATUS ">>> Using user-specified Lumina library: ${LUMINA_LIBRARY} (static=${LUMINA_IS_STATIC})")
else()
    set(LUMINA_LIB_SEARCH_PATHS
        ${LUMINA_ROOT}/lib
        ${LUMINA_ROOT}/lib64
        /usr/local/lib
        /usr/local/lib64
        /usr/lib
        /usr/lib64
    )

    # List files in lib directories to help diagnose
    foreach(_dir ${LUMINA_LIB_SEARCH_PATHS})
        if(EXISTS "${_dir}")
            file(GLOB _libs "${_dir}/liblumina*")
            if(_libs)
                message(STATUS "Lumina libs in ${_dir}: ${_libs}")
            endif()
        endif()
    endforeach()

    # 1. Try static archive first (best for fat .so)
    set(_ORIG_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    find_library(LUMINA_STATIC_LIB
        NAMES lumina
        PATHS ${LUMINA_LIB_SEARCH_PATHS}
        NO_DEFAULT_PATH
    )
    if(NOT LUMINA_STATIC_LIB)
        find_library(LUMINA_STATIC_LIB NAMES lumina)
    endif()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ORIG_SUFFIXES})

    # 2. Fall back to shared library
    find_library(LUMINA_SHARED_LIB
        NAMES lumina
        PATHS ${LUMINA_LIB_SEARCH_PATHS}
        NO_DEFAULT_PATH
    )
    if(NOT LUMINA_SHARED_LIB)
        find_library(LUMINA_SHARED_LIB NAMES lumina)
    endif()

    message(STATUS "Static library search result : ${LUMINA_STATIC_LIB}")
    message(STATUS "Shared library search result : ${LUMINA_SHARED_LIB}")

    if(LUMINA_STATIC_LIB)
        set(LUMINA_LIBRARY ${LUMINA_STATIC_LIB})
        set(LUMINA_IS_STATIC TRUE)
        message(STATUS ">>> Using Lumina STATIC library: ${LUMINA_LIBRARY}")
    elseif(LUMINA_SHARED_LIB)
        set(LUMINA_LIBRARY ${LUMINA_SHARED_LIB})
        set(LUMINA_IS_STATIC FALSE)
        message(STATUS ">>> Using Lumina SHARED library: ${LUMINA_LIBRARY}")
    else()
        message(FATAL_ERROR
            "Lumina library not found. Set LUMINA_ROOT or LUMINA_LIBRARY.\n"
            "  Searched in: ${LUMINA_LIB_SEARCH_PATHS}\n"
            "  Or specify directly: -DLUMINA_LIBRARY=/path/to/liblumina.so")
    endif()
endif()

# ==================== Build JNI shared library ====================
add_library(paimon_lumina_jni SHARED paimon_lumina_jni.cpp)

target_include_directories(paimon_lumina_jni PRIVATE ${LUMINA_INCLUDE_DIR})

if(LUMINA_IS_STATIC)
    # Static: embed all symbols into the fat .so
    target_link_libraries(paimon_lumina_jni PRIVATE
        -Wl,--whole-archive ${LUMINA_LIBRARY} -Wl,--no-whole-archive
        pthread dl m
    )
    target_link_options(paimon_lumina_jni PRIVATE
        -static-libstdc++
        -static-libgcc
        "-Wl,--exclude-libs,ALL"
    )
else()
    # Shared: link at build time, liblumina.so must be present at runtime
    get_filename_component(LUMINA_LIB_DIR ${LUMINA_LIBRARY} DIRECTORY)
    target_link_libraries(paimon_lumina_jni PRIVATE
        -L${LUMINA_LIB_DIR} -llumina
        pthread dl m
    )
endif()
