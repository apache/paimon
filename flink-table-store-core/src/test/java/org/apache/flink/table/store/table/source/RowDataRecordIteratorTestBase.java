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

package org.apache.flink.table.store.table.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.ReusingTestData;
import org.apache.flink.table.store.file.utils.TestReusingRecordReader;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Tests for {@link RecordReader.RecordIterator} of {@link RowData}. */
public abstract class RowDataRecordIteratorTestBase {

    protected void testIterator(
            List<ReusingTestData> input,
            Function<RecordReader.RecordIterator<KeyValue>, RecordReader.RecordIterator<RowData>>
                    rowDataIteratorSupplier,
            BiConsumer<RowData, Integer> resultChecker)
            throws Exception {
        int cnt = 0;
        TestReusingRecordReader recordReader = new TestReusingRecordReader(input);
        while (true) {
            RecordReader.RecordIterator<KeyValue> kvIterator = recordReader.readBatch();
            if (kvIterator == null) {
                break;
            }
            RecordReader.RecordIterator<RowData> rowDataIterator =
                    rowDataIteratorSupplier.apply(kvIterator);
            RowData rowData;
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
