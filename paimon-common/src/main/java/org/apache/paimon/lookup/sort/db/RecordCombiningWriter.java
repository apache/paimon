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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.memory.MemorySlice;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.lookup.sort.db.LocalKvDb.isTombstone;

/** Combines adjacent records before forwarding them to an SST writer. */
final class RecordCombiningWriter {

    private static final byte[] TOMBSTONE = new byte[0];

    @Nullable private final LocalKvDb.MergeOperator mergeOperator;
    private final RecordConsumer consumer;

    @Nullable private MemorySlice pendingKey;
    private final List<MemorySlice> pendingKeys;
    private final List<byte[]> pendingValues;

    RecordCombiningWriter(
            @Nullable LocalKvDb.MergeOperator mergeOperator, RecordConsumer consumer) {
        this.mergeOperator = mergeOperator;
        this.consumer = consumer;
        this.pendingKeys = new ArrayList<>();
        this.pendingValues = new ArrayList<>();
    }

    void put(MemorySlice key, byte[] value) throws IOException {
        if (mergeOperator == null) {
            consumer.accept(key, value);
            return;
        }

        if (isTombstone(value)) {
            flushPending();
            consumer.accept(key, value);
            return;
        }

        if (pendingKey == null) {
            startGroup(key, value);
        } else if (mergeOperator.canMerge(pendingKey, key)) {
            pendingKeys.add(MemorySlice.wrap(key.copyBytes()));
            pendingValues.add(value);
        } else {
            flushPending();
            startGroup(key, value);
        }
    }

    void finish() throws IOException {
        flushPending();
    }

    private void startGroup(MemorySlice key, byte[] value) {
        pendingKey = MemorySlice.wrap(key.copyBytes());
        pendingKeys.add(pendingKey);
        pendingValues.add(value);
    }

    private void flushPending() throws IOException {
        if (pendingKey == null) {
            return;
        }

        byte[] value =
                pendingValues.size() == 1
                        ? pendingValues.get(0)
                        : mergeOperator.merge(pendingValues);
        consumer.accept(pendingKey, value);
        for (int i = 1; i < pendingKeys.size(); i++) {
            consumer.accept(pendingKeys.get(i), TOMBSTONE);
        }
        pendingKey = null;
        pendingKeys.clear();
        pendingValues.clear();
    }

    /** Callback receiving one combined record. */
    interface RecordConsumer {

        void accept(MemorySlice key, byte[] value) throws IOException;
    }
}
