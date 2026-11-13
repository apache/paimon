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

package org.apache.paimon.append;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForceSingleBatchReader}. */
class ForceSingleBatchReaderTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConsecutiveEmptyBatches(boolean withRecords) throws Exception {
        int batchCount = 60002;
        AtomicInteger released = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        RecordReader<InternalRow> input =
                new RecordReader<InternalRow>() {
                    private int nextBatch;

                    @Override
                    public RecordIterator<InternalRow> readBatch() {
                        assertThat(released.get()).isEqualTo(nextBatch);
                        if (nextBatch == batchCount) {
                            return null;
                        }
                        int batchIndex = nextBatch++;
                        return new RecordIterator<InternalRow>() {
                            private boolean returned;

                            @Override
                            public InternalRow next() {
                                if (!returned
                                        && withRecords
                                        && (batchIndex == 20000 || batchIndex == 40001)) {
                                    returned = true;
                                    return GenericRow.of(batchIndex);
                                }
                                return null;
                            }

                            @Override
                            public void releaseBatch() {
                                released.incrementAndGet();
                            }
                        };
                    }

                    @Override
                    public void close() {
                        closed.set(true);
                    }
                };

        List<Integer> values = new ArrayList<>();
        try (ForceSingleBatchReader reader = new ForceSingleBatchReader(input)) {
            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            InternalRow row;
            while ((row = batch.next()) != null) {
                values.add(row.getInt(0));
            }
            batch.releaseBatch();
            assertThat(reader.readBatch()).isNull();
        }
        if (withRecords) {
            assertThat(values).containsExactly(20000, 40001);
        } else {
            assertThat(values).isEmpty();
        }
        assertThat(released.get()).isEqualTo(batchCount);
        assertThat(closed.get()).isTrue();
    }
}
