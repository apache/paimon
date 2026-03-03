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

# ==================== Locate Lumina static library ====================
# A fat .so requires a static archive (.a) to embed via --whole-archive.
# User can specify the exact path via -DLUMINA_LIBRARY=/path/to/liblumina.a

if(NOT LUMINA_LIBRARY)
    # Auto-search for liblumina.a
    set(_ORIG_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    find_library(LUMINA_LIBRARY
        NAMES lumina
        PATHS
            ${LUMINA_ROOT}/lib
            ${LUMINA_ROOT}/lib64
            /usr/local/lib
            /usr/local/lib64
            /usr/lib
            /usr/lib64
        NO_DEFAULT_PATH
    )
    if(NOT LUMINA_LIBRARY)
        find_library(LUMINA_LIBRARY NAMES lumina)
    endif()
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_ORIG_SUFFIXES})
endif()

# Diagnostic: list files in lib directories
foreach(_dir ${LUMINA_ROOT}/lib ${LUMINA_ROOT}/lib64)
    if(EXISTS "${_dir}")
        file(GLOB _libs "${_dir}/liblumina*")
        if(_libs)
            message(STATUS "Lumina libs in ${_dir}: ${_libs}")
        endif()
    endif()
endforeach()

if(NOT LUMINA_LIBRARY OR NOT EXISTS "${LUMINA_LIBRARY}")
    message(FATAL_ERROR
        "Lumina static library (liblumina.a) not found.\n"
        "  A fat .so requires liblumina.a (static archive).\n"
        "  Set -DLUMINA_LIBRARY=/path/to/liblumina.a\n"
        "  Or ensure liblumina.a is in ${LUMINA_ROOT}/lib/")
endif()

message(STATUS ">>> Using Lumina library: ${LUMINA_LIBRARY}")

# ==================== Build fat JNI shared library ====================
add_library(paimon_lumina_jni SHARED paimon_lumina_jni.cpp)

target_include_directories(paimon_lumina_jni PRIVATE ${LUMINA_INCLUDE_DIR})

# Embed all Lumina symbols into a single fat .so via --whole-archive
target_link_libraries(paimon_lumina_jni PRIVATE
    -Wl,--whole-archive ${LUMINA_LIBRARY} -Wl,--no-whole-archive
    pthread dl m
)
target_link_options(paimon_lumina_jni PRIVATE
    -static-libstdc++
    -static-libgcc
    "-Wl,--exclude-libs,ALL"
)
