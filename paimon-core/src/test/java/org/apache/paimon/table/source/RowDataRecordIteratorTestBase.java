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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.ReusingTestData;
import org.apache.paimon.utils.TestReusingRecordReader;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordReader.RecordIterator} of {@link InternalRow}. */
public abstract class RowDataRecordIteratorTestBase {

    protected void testIterator(
            List<ReusingTestData> input,
            Function<
                            RecordReader.RecordIterator<KeyValue>,
                            RecordReader.RecordIterator<InternalRow>>
                    rowDataIteratorSupplier,
            BiConsumer<InternalRow, Integer> resultChecker)
            throws Exception {
        int cnt = 0;
        TestReusingRecordReader recordReader = new TestReusingRecordReader(input);
        while (true) {
            RecordReader.RecordIterator<KeyValue> kvIterator = recordReader.readBatch();
            if (kvIterator == null) {
                break;
            }

            RecordReader.RecordIterator<KeyValue> assertKvIterator =
                    new RecordReader.RecordIterator<KeyValue>() {

                        KeyValue previous;

                        @Override
                        public KeyValue next() throws IOException {
                            // check
                            if (previous != null) {
                                assertThat(previous.key().getRowKind()).isEqualTo(RowKind.INSERT);
                                assertThat(previous.value().getRowKind()).isEqualTo(RowKind.INSERT);
                            }
                            previous = kvIterator.next();
                            return previous;
                        }

                        @Override
                        public void releaseBatch() {
                            kvIterator.releaseBatch();
                        }
                    };

            RecordReader.RecordIterator<InternalRow> rowDataIterator =
                    rowDataIteratorSupplier.apply(assertKvIterator);
            InternalRow rowData;
            while (true) {
                rowData = rowDataIterator.next();
                if (rowData == null) {
                    break;
                }
                resultChecker.accept(rowData, cnt);
                cnt++;
            }
            rowDataIterator.releaseBatch();
        }
        recordReader.close();
        recordReader.assertCleanUp();
    }
}
