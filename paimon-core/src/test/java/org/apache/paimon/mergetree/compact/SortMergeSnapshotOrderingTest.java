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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for snapshot-ordering in sort-merge readers. With {@code sequence.snapshot-ordering}, the
 * commit snapshot id is carried in each record's {@code sequenceNumber} (stamped at read time for
 * APPEND files), so the sort-merge readers need no snapshot-specific branch: comparing by {@code
 * sequenceNumber} already makes records from later snapshots win.
 */
public class SortMergeSnapshotOrderingTest {

    private static final Comparator<org.apache.paimon.data.InternalRow> KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    private static final RowType VALUE_TYPE = RowType.of(DataTypes.INT());

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testLaterSnapshotWins(SortEngine sortEngine) throws IOException {
        // seq carries the snapshot id: snapshot 6 wins over snapshot 5.
        KeyValue winner = merge(sortEngine, kv(1, 5, 999), kv(1, 6, 1));
        assertThat(winner.value().getInt(0)).isEqualTo(1);
        assertThat(winner.sequenceNumber()).isEqualTo(6);
    }

    @ParameterizedTest
    @EnumSource(SortEngine.class)
    public void testHigherSequenceWins(SortEngine sortEngine) throws IOException {
        KeyValue winner = merge(sortEngine, kv(1, 100, 100), kv(1, 50, 50));
        assertThat(winner.value().getInt(0)).isEqualTo(100);
    }

    private static KeyValue kv(int key, long seq, int value) {
        return new KeyValue()
                .replace(GenericRow.of(key), seq, RowKind.INSERT, GenericRow.of(value));
    }

    private static KeyValue merge(SortEngine sortEngine, KeyValue... kvs) throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>();
        for (KeyValue kv : kvs) {
            readers.add(new SingleKvReader(kv));
        }

        MergeFunctionWrapper<KeyValue> wrapper =
                new ReducerMergeFunctionWrapper(DeduplicateMergeFunction.factory().create());

        RecordReader<KeyValue> reader =
                SortMergeReader.createSortMergeReader(
                        readers, KEY_COMPARATOR, null, wrapper, sortEngine);

        RecordReader.RecordIterator<KeyValue> batch = reader.readBatch();
        assertThat(batch).isNotNull();
        KeyValue result = batch.next();
        assertThat(result).isNotNull();
        assertThat(batch.next()).isNull();
        batch.releaseBatch();
        reader.close();
        return result;
    }

    private static class SingleKvReader implements RecordReader<KeyValue> {
        private KeyValue kv;

        SingleKvReader(KeyValue kv) {
            this.kv = kv;
        }

        @Nullable
        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (kv == null) {
                return null;
            }
            KeyValue toReturn = kv;
            kv = null;
            return new RecordIterator<KeyValue>() {
                private boolean returned = false;

                @Nullable
                @Override
                public KeyValue next() {
                    if (returned) {
                        return null;
                    }
                    returned = true;
                    return toReturn;
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() {}
    }
}
