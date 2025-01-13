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

package org.apache.paimon.mergetree.localmerge;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import java.io.IOException;
import java.util.function.Consumer;

/** A {@link LocalMerger} which stores records in {@link SortBufferWriteBuffer}. */
public class SortBufferLocalMerger implements LocalMerger {

    private final SortBufferWriteBuffer sortBuffer;
    private final RecordComparator keyComparator;
    private final MergeFunction<KeyValue> mergeFunction;

    private long recordCount;

    public SortBufferLocalMerger(
            SortBufferWriteBuffer sortBuffer,
            RecordComparator keyComparator,
            MergeFunction<KeyValue> mergeFunction) {
        this.sortBuffer = sortBuffer;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.recordCount = 0;
    }

    @Override
    public boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException {
        return sortBuffer.put(recordCount++, rowKind, key, value);
    }

    @Override
    public int size() {
        return sortBuffer.size();
    }

    @Override
    public void forEach(Consumer<InternalRow> consumer) throws IOException {
        sortBuffer.forEach(
                keyComparator,
                mergeFunction,
                null,
                kv -> {
                    InternalRow row = kv.value();
                    row.setRowKind(kv.valueKind());
                    consumer.accept(row);
                });
    }

    @Override
    public void clear() {
        sortBuffer.clear();
    }
}
