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

import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;

import java.util.zip.CRC32;

/** Utils for sort lookup store. */
public class SortLookupStoreUtils {
    public static int crc32c(MemorySlice data, BlockCompressionType type) {
        CRC32 crc = new CRC32();
        crc.update(data.getHeapMemory(), data.offset(), data.length());
        crc.update(type.persistentId() & 0xFF);
        return (int) crc.getValue();
    }

    public static int crc32c(MemorySegment data, BlockCompressionType type) {
        CRC32 crc = new CRC32();
        crc.update(data.getHeapMemory(), 0, data.size());
        crc.update(type.persistentId() & 0xFF);
        return (int) crc.getValue();
    }
}
