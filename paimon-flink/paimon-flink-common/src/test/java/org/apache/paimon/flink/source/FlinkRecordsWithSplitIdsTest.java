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

import org.apache.paimon.flink.source.FileStoreSourceReaderTest.DummyMetricGroup;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.SingletonResultIterator;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkRecordsWithSplitIds}. */
public class FlinkRecordsWithSplitIdsTest {

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
    public void testEmitRecord() {
        RecordsWithSplitIds<BulkFormat.RecordIterator<RowData>> records =
                FlinkRecordsWithSplitIds.forRecords("", iter);
        records.nextSplit();

        BulkFormat.RecordIterator<RowData> iterator = records.nextRecordFromSplit();
        assertThat(iterator).isNotNull();
        FlinkRecordsWithSplitIds.emitRecord(
                new TestingReaderContext(),
                iterator,
                output,
                state,
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                null);
        assertThat(output.getEmittedRecords()).containsExactly(rows);
        assertThat(state.recordsToSkip()).isEqualTo(2);
        assertThat(records.nextRecordFromSplit()).isNull();
    }

    @Test
    void testEmptySplits() {
        final String split = "empty";
        final FlinkRecordsWithSplitIds records = FlinkRecordsWithSplitIds.finishedSplit(split);

        assertThat(records.finishedSplits()).isEqualTo(Collections.singleton(split));
    }

    @Test
    void testMoveToFirstSplit() {
        final String splitId = "splitId";
        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords(splitId, new SingletonResultIterator<>());

        final String firstSplitId = records.nextSplit();

        assertThat(splitId).isEqualTo(firstSplitId);
    }

    @Test
    void testMoveToSecondSplit() {
        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("splitId", new SingletonResultIterator<>());
        records.nextSplit();

        final String secondSplitId = records.nextSplit();

        assertThat(secondSplitId).isNull();
    }

    @Test
    void testRecordsFromFirstSplit() {
        final SingletonResultIterator<RowData> iter = new SingletonResultIterator<>();
        iter.set(GenericRowData.of("test"), 18, 99);
        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("splitId", iter);
        records.nextSplit();

        final BulkFormat.RecordIterator<RowData> recAndPos = records.nextRecordFromSplit();

        assertThat(recAndPos).isSameAs(iter);
        assertThat(records.nextRecordFromSplit()).isNull();
    }

    @Test
    void testRecordsInitiallyIllegal() {
        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("splitId", new SingletonResultIterator<>());

        assertThatThrownBy(records::nextRecordFromSplit).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecordsOnSecondSplitIllegal() {
        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("splitId", new SingletonResultIterator<>());
        records.nextSplit();
        records.nextSplit();

        assertThatThrownBy(records::nextRecordFromSplit).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecycleExhaustedBatch() {
        final AtomicBoolean recycled = new AtomicBoolean(false);
        final SingletonResultIterator<RowData> iter =
                new SingletonResultIterator<>(() -> recycled.set(true));
        iter.set(GenericRowData.of(), 1L, 2L);

        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("test split", iter);
        records.nextSplit();
        records.nextRecordFromSplit();

        // make sure we exhausted the iterator
        assertThat(records.nextRecordFromSplit()).isNull();
        assertThat(records.nextSplit()).isNull();

        records.recycle();
        assertThat(recycled.get()).isTrue();
    }

    @Test
    void testRecycleNonExhaustedBatch() {
        final AtomicBoolean recycled = new AtomicBoolean(false);
        final SingletonResultIterator<RowData> iter =
                new SingletonResultIterator<>(() -> recycled.set(true));
        iter.set(GenericRowData.of(), 1L, 2L);

        final FlinkRecordsWithSplitIds records =
                FlinkRecordsWithSplitIds.forRecords("test split", iter);
        records.nextSplit();

        records.recycle();
        assertThat(recycled.get()).isTrue();
    }
}
