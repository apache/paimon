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

package org.apache.paimon.manifest;

/** The Source of a file. */
public enum FileSource {

    /** The file from new input. */
    APPEND((byte) 0),

    /** The file from compaction. */
    COMPACT((byte) 1);

    private final byte value;

    FileSource(byte value) {
        this.value = value;
    }

    public byte toByteValue() {
        return value;
    }

    public static FileSource fromByteValue(byte value) {
        switch (value) {
            case 0:
                return APPEND;
            case 1:
                return COMPACT;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for value kind.");
        }
    }
}
