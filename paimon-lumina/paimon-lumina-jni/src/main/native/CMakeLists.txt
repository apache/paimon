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

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Bake RPATH=$ORIGIN into the .so at link time (no patchelf needed for JNI lib)
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH "\$ORIGIN")

# ==================== Find JNI headers (manual, avoids FindJNI compile check) ====================
if(JAVA_HOME AND NOT DEFINED ENV{JAVA_HOME})
    set(ENV{JAVA_HOME} "${JAVA_HOME}")
endif()

if(NOT DEFINED ENV{JAVA_HOME})
    message(FATAL_ERROR "JAVA_HOME is required. Set -DJAVA_HOME=/path/to/jdk")
endif()

set(JNI_INCLUDE_DIRS "$ENV{JAVA_HOME}/include")
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND JNI_INCLUDE_DIRS "$ENV{JAVA_HOME}/include/linux")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND JNI_INCLUDE_DIRS "$ENV{JAVA_HOME}/include/darwin")
endif()

if(NOT EXISTS "$ENV{JAVA_HOME}/include/jni.h")
    message(FATAL_ERROR "jni.h not found at $ENV{JAVA_HOME}/include/jni.h")
endif()

message(STATUS "JNI headers : ${JNI_INCLUDE_DIRS}")
include_directories(${JNI_INCLUDE_DIRS})

# ==================== Find Lumina (LUMINA_ROOT required) ====================
if(NOT LUMINA_ROOT AND DEFINED ENV{LUMINA_ROOT})
    set(LUMINA_ROOT $ENV{LUMINA_ROOT})
endif()

if(NOT LUMINA_ROOT)
    message(FATAL_ERROR "LUMINA_ROOT is required. Set -DLUMINA_ROOT=/path/to/lumina")
endif()

set(LUMINA_INCLUDE_DIR "${LUMINA_ROOT}/include")
set(LUMINA_LIBRARY "${LUMINA_ROOT}/lib/liblumina.so")

if(NOT EXISTS "${LUMINA_INCLUDE_DIR}/lumina/api/LuminaBuilder.h")
    message(FATAL_ERROR "Lumina headers not found at ${LUMINA_INCLUDE_DIR}")
endif()

if(NOT EXISTS "${LUMINA_LIBRARY}")
    message(FATAL_ERROR "liblumina.so not found at ${LUMINA_LIBRARY}")
endif()

message(STATUS "Lumina headers : ${LUMINA_INCLUDE_DIR}")
message(STATUS "Lumina library : ${LUMINA_LIBRARY}")

# ==================== Build JNI shared library ====================
add_library(paimon_lumina_jni SHARED paimon_lumina_jni.cpp)

set_target_properties(paimon_lumina_jni PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

target_include_directories(paimon_lumina_jni PRIVATE ${LUMINA_INCLUDE_DIR})

target_link_libraries(paimon_lumina_jni PRIVATE
    -L${LUMINA_ROOT}/lib -llumina
    pthread dl m
)
target_link_options(paimon_lumina_jni PRIVATE
    -static-libstdc++
    -static-libgcc
)
