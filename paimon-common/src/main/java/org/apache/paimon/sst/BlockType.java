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

package org.apache.paimon.sst;

/** Type of Block. */
public enum BlockType {
    DATA((byte) 0),
    LEAF_INDEX((byte) 1),
    INTERMEDIATE_INDEX((byte) 2),
    ROOT_INDEX((byte) 3),
    BLOOM_FILTER((byte) 4),
    FILE_INFO((byte) 5);

    private final byte value;

    BlockType(byte value) {
        this.value = value;
    }

    public byte toByte() {
        return value;
    }

    public boolean isIndex() {
        return value >= 1 && value <= 3;
    }

    public static BlockType fromByte(byte value) {
        for (BlockType blockType : values()) {
            if (blockType.value == value) {
                return blockType;
            }
        }
        throw new IllegalStateException("Illegal block type: " + value);
    }
}
