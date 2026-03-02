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

find_library(LUMINA_LIBRARY
    NAMES lumina
    PATHS
        ${LUMINA_ROOT}/lib
        /usr/local/lib
        /usr/local/lib64
        /usr/lib
        /usr/lib64
)

if(NOT LUMINA_LIBRARY)
    message(FATAL_ERROR "Lumina library not found. Set LUMINA_ROOT.")
endif()
message(STATUS "Found Lumina library: ${LUMINA_LIBRARY}")

# ==================== Build JNI shared library ====================
add_library(paimon_lumina_jni SHARED paimon_lumina_jni.cpp)

target_include_directories(paimon_lumina_jni PRIVATE ${LUMINA_INCLUDE_DIR})

# Link everything into one .so:
#   -Wl,--whole-archive  : pull all symbols from liblumina.a / liblumina.so
#   -static-libstdc++ -static-libgcc : embed C++ runtime
#   -Wl,--exclude-libs,ALL : hide all third-party symbols
target_link_libraries(paimon_lumina_jni PRIVATE
    -Wl,--whole-archive ${LUMINA_LIBRARY} -Wl,--no-whole-archive
    pthread dl m
)
target_link_options(paimon_lumina_jni PRIVATE
    -static-libstdc++
    -static-libgcc
    "-Wl,--exclude-libs,ALL"
)
