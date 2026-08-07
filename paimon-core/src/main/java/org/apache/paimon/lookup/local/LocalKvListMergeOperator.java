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

package org.apache.paimon.lookup.local;

import org.apache.paimon.lookup.sort.db.LocalKvDb;
import org.apache.paimon.memory.MemorySlice;

import java.io.IOException;
import java.util.List;

/**
 * Combines ListState fragments with the same logical key when writing SST files.
 *
 * <p>When TTL is enabled, merging includes expired fragments and re-encodes the result with a new
 * expiration time, matching RocksDB's TTL merge behavior.
 */
final class LocalKvListMergeOperator implements LocalKvDb.MergeOperator {

    private final LocalKvValueCodec valueCodec;
    private final ThreadLocal<LocalKvListValueCodec> listValueCodec =
            ThreadLocal.withInitial(LocalKvListValueCodec::new);

    LocalKvListMergeOperator(LocalKvValueCodec valueCodec) {
        this.valueCodec = valueCodec;
    }

    @Override
    public boolean canMerge(MemorySlice firstKey, MemorySlice nextKey) {
        if (firstKey.length() < Long.BYTES || firstKey.length() != nextKey.length()) {
            return false;
        }

        int logicalKeyLength = firstKey.length() - Long.BYTES;
        for (int i = 0; i < logicalKeyLength; i++) {
            if (firstKey.readByte(i) != nextKey.readByte(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public byte[] merge(List<byte[]> values) throws IOException {
        // Foreground flush and background compaction can invoke the operator concurrently.
        return listValueCodec.get().merge(values, valueCodec);
    }
}
