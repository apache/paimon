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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.source.RecordsFunction.IterateRecordsFunction;
import org.apache.paimon.flink.source.RecordsFunction.SingleRecordsFunction;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RecordsFunction}. */
public class RecordsFunctionTest {

    private RowData[] rows;
    private ArrayResultIterator<RowData> iter;
    private TestingReaderOutput<RowData> output;
    private FileStoreSourceSplitState state;

    @BeforeEach
    public void beforeEach() {
        iter = new ArrayResultIterator<>();
        rows = new RowData[] {GenericRowData.of(1, 1), GenericRowData.of(2, 2)};
        iter.set(rows, rows.length, CheckpointedPosition.NO_OFFSET, 0);
        output = new TestingReaderOutput<>();
        state = new FileStoreSourceSplitState(new FileStoreSourceSplit("", null));
    }

    @Test
    public void testSingle() {
        SingleRecordsFunction function = RecordsFunction.forSingle();
        RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> records =
                function.createRecords("", iter);
        records.nextSplit();

        BulkFormat.RecordIterator<RowData> iterator = records.nextRecordFromSplit();
        assertThat(iterator).isNotNull();
        function.emitRecord(iterator, output, state);
        assertThat(output.getEmittedRecords()).containsExactly(rows);
        assertThat(state.recordsToSkip()).isEqualTo(2);
        assertThat(records.nextRecordFromSplit()).isNull();
    }

    @Test
    public void testIterate() {
        IterateRecordsFunction function = RecordsFunction.forIterate();
        RecordsWithSplitIds<RecordAndPosition<RowData>> records = function.createRecords("", iter);
        records.nextSplit();

        RecordAndPosition<RowData> record = records.nextRecordFromSplit();
        assertThat(record).isNotNull();
        function.emitRecord(record, output, state);
        assertThat(output.getEmittedRecords()).containsExactly(rows[0]);
        assertThat(state.recordsToSkip()).isEqualTo(1);

        record = records.nextRecordFromSplit();
        assertThat(record).isNotNull();
        function.emitRecord(record, output, state);
        assertThat(output.getEmittedRecords()).containsExactly(rows);
        assertThat(state.recordsToSkip()).isEqualTo(2);

        assertThat(records.nextRecordFromSplit()).isNull();
    }
}
