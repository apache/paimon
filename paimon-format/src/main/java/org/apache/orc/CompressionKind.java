/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc;

/* This file is based on source code from the ORC Project (http://orc.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An enumeration that lists the generic compression algorithms that can be applied to ORC files.
 *
 * <p>NOTE: The file was copied and modified to support zstd-jni. This feature is only supported in
 * ORC 2.0, but 2.0 only supports JDK17. We need to support JDK8.
 */
public enum CompressionKind {
    NONE,
    ZLIB,
    SNAPPY,
    LZO,
    LZ4,
    ZSTD,
    BROTLI
}
