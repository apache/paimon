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

package org.apache.paimon.lookup.sort;

/** Aligned type for block. */
public enum BlockAlignedType {
    ALIGNED((byte) 0),
    UNALIGNED((byte) 1);

    private final byte b;

    BlockAlignedType(byte b) {
        this.b = b;
    }

    public byte toByte() {
        return b;
    }

    public static BlockAlignedType fromByte(byte b) {
        for (BlockAlignedType type : BlockAlignedType.values()) {
            if (type.toByte() == b) {
                return type;
            }
        }
        throw new IllegalStateException("Illegal block aligned type: " + b);
    }
}
