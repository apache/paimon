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

package org.apache.paimon.flink.pipeline.cdc.source.reader;

import org.apache.paimon.KeyValue;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.pipeline.cdc.source.CDCSource;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceReaderTest.DummyMetricGroup;
import org.apache.paimon.flink.source.TestChangelogDataReadWrite;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RecordWriter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.BulkFormat.RecordIterator;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newSourceSplit;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CDCSourceSplitReader}. */
public class CDCSourceSplitReaderTest {

    @TempDir java.nio.file.Path tempDir;

    private static final String DATABASE = "default";
    private static final String TABLE = "test_table";
    private static final Schema SCHEMA =
            new Schema(
                    toDataType(
                                    new RowType(
                                            Collections.singletonList(
                                                    new RowType.RowField("v", new BigIntType()))))
                            .getFields(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    null);

    private String tablePath;

    @BeforeEach
    public void beforeEach() throws Exception {
        Options options = new Options();
        options.setString("warehouse", tempDir.toUri().toString());
        CatalogContext catalogContext = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        catalog.createDatabase(DATABASE, true);
        catalog.createTable(Identifier.create(DATABASE, TABLE), SCHEMA, false);
        tablePath = catalog.getTable(Identifier.create(DATABASE, TABLE)).options().get("path");
    }

    @Test
    public void testPrimaryKey() throws Exception {
        innerTestOnce(0);
    }

    @Test
    public void testPrimaryKeySkip() throws Exception {
        innerTestOnce(4);
    }

    @Test
    public void testSplitReaderWakeupAble() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, 0));
        reader.fetch();

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                // block on object pool
                                reader.fetch();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        thread.start();
        thread.join(20000);
        assertThat(thread.isAlive()).isTrue();
        reader.wakeUp();
        thread.join(15000);
        assertThat(thread.isAlive()).isFalse();
    }

    private CDCSourceSplitReader createReader(TableRead tableRead) {
        return createReader(tableRead, Collections.emptyList());
    }

    private CDCSourceSplitReader createReader(
            TableRead tableRead, List<SchemaChangeEvent> schemaChangeEvents) {
        return new TestCDCSourceSplitReader(
                new FileStoreSourceReaderMetrics(new DummyMetricGroup()),
                tableRead,
                schemaChangeEvents);
    }

    private void innerTestOnce(int skip) throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, skip));

        RecordsWithSplitIds<RecordIterator<Event>> records = reader.fetch();

        List<Tuple2<RowKind, Long>> expected =
                input.stream()
                        .map(t -> new Tuple2<>(RowKind.INSERT, t.f1))
                        .collect(Collectors.toList());

        List<Tuple2<RowKind, Long>> result = readRecords(records, "id1", skip);
        assertThat(result).isEqualTo(expected.subList(skip, expected.size()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testPrimaryKeyWithDelete() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input = kvs();
        RecordWriter<KeyValue> writer = rw.createMergeTreeWriter(row(1), 0);
        for (Tuple2<Long, Long> tuple2 : input) {
            writer.write(
                    new KeyValue()
                            .replace(
                                    GenericRow.of(tuple2.f0),
                                    org.apache.paimon.types.RowKind.INSERT,
                                    GenericRow.of(tuple2.f1)));
        }
        writer.write(
                new KeyValue()
                        .replace(
                                GenericRow.of(222L),
                                org.apache.paimon.types.RowKind.DELETE,
                                GenericRow.of(333L)));
        List<DataFileMeta> files = writer.prepareCommit(true).newFilesIncrement().newFiles();
        writer.close();

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, true));
        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();

        List<Tuple2<RowKind, Long>> expected =
                input.stream()
                        .map(t -> new Tuple2<>(RowKind.INSERT, t.f1))
                        .collect(Collectors.toList());
        expected.add(new Tuple2<>(RowKind.DELETE, 333L));

        List<Tuple2<RowKind, Long>> result = readRecords(records, "id1", 0);
        assertThat(result).isEqualTo(expected);

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testMultipleBatchInSplit() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Long, Long>> input2 = kvs(6);
        List<DataFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                0,
                input1.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                6,
                input2.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testSchemaChangeEventOnlyEmittedOnceInMultipleBatchSplit() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey(), schemaChangeEvents());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Long, Long>> input2 = kvs(6);
        List<DataFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertThat(readEventTypes(records, "id1"))
                .containsExactly(
                        SchemaChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class);

        records = reader.fetch();
        assertThat(readEventTypes(records, "id1"))
                .containsExactly(
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class);

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testSchemaChangeEventDoesNotAdvanceRecordsToSkip() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey(), schemaChangeEvents());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertThat(readRecordSkipCounts(records, "id1"))
                .containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);

        reader.close();
    }

    @Test
    public void testRestoreWithSchemaChangeEventsDoesNotReemitSchemaEvent() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey(), schemaChangeEvents());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Long, Long>> input2 = kvs(6);
        List<DataFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, false, input1.size(), 1L));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(records, null, "id1", input1.size(), Collections.emptyList());

        records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                input1.size(),
                input2.stream().map(t -> t.f1).collect(Collectors.toList()));

        reader.close();
    }

    @Test
    public void testRestoreWithPartialSchemaChangeEventsSkipsEmittedEvents() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader =
                createReader(rw.createReadWithKey(), multipleSchemaChangeEvents());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, false, 0L, 1L));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertThat(readEventTypes(records, "id1"))
                .containsExactly(
                        SchemaChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class);

        reader.close();
    }

    @Test
    public void testRestoreWithAllSchemaChangeEventsSkippedReadsDataRows() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader =
                createReader(rw.createReadWithKey(), multipleSchemaChangeEvents());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, false, 0L, 2L));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertThat(readEventTypes(records, "id1"))
                .containsExactly(
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class);

        reader.close();
    }

    @Test
    public void testRestoreWithInvalidSchemaChangeEventSkipCountFails() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey(), schemaChangeEvents());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, false, 0L, 2L));

        assertThatThrownBy(reader::fetch)
                .hasMessageContaining("Invalid schema change event skip count 2");

        reader.close();
    }

    @Test
    public void testRestoreFromLegacySplitWithDataProgressSkipsSchemaChangeEvents()
            throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey(), schemaChangeEvents());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        TableAwareFileStoreSourceSplit split = newSourceSplit("id1", row(1), 0, files, 1L);
        assignSplit(reader, legacySplit(split));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertThat(readEventTypes(records, "id1"))
                .containsExactly(
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class,
                        DataChangeEvent.class);

        reader.close();
    }

    @Test
    public void testRestore() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, 3));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                3,
                input.subList(3, input.size()).stream()
                        .map(t -> t.f1)
                        .collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testRestoreMultipleBatchInSplit() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Long, Long>> input2 = kvs(6);
        List<DataFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, newSourceSplit("id1", row(1), 0, files, 7));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                7,
                Stream.concat(input1.stream(), input2.stream())
                        .skip(7)
                        .map(t -> t.f1)
                        .collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testMultipleSplits() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files1 = rw.writeFiles(row(1), 0, input1);
        assignSplit(reader, newSourceSplit("id1", row(1), 0, files1));

        List<Tuple2<Long, Long>> input2 = kvs();
        List<DataFileMeta> files2 = rw.writeFiles(row(2), 1, input2);
        assignSplit(reader, newSourceSplit("id2", row(2), 1, files2));

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                0,
                input1.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        records = reader.fetch();
        assertRecords(
                records,
                null,
                "id2",
                0,
                input2.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id2", "id2", 0, null);

        reader.close();
    }

    @Test
    public void testNoSplit() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());
        assertThatThrownBy(reader::fetch).hasMessageContaining("no split remaining");
        reader.close();
    }

    @Test
    public void testRecycleIteratorWhenReleaseBatchFails() throws Exception {
        CDCSourceSplitReader reader =
                createReader(
                        new TestingTableRead(
                                new SingleBatchRecordReader(new FailingReleaseIterator())));
        try {
            assignSplit(
                    reader,
                    new TableAwareFileStoreSourceSplit(
                            "id1",
                            new TestingSplit(),
                            0,
                            Identifier.create(DATABASE, TABLE),
                            1L,
                            1L));

            RecordsWithSplitIds<RecordIterator<Event>> records = reader.fetch();
            assertThatThrownBy(records::recycle).hasMessageContaining("release failed");

            RecordsWithSplitIds<RecordIterator<Event>> finishedRecords =
                    fetchWithoutWaitingForPool(reader);
            assertThat(finishedRecords.finishedSplits()).isEqualTo(Collections.singleton("id1"));
        } finally {
            reader.close();
        }
    }

    @Test
    public void testCloseReleasesCurrentFirstBatch() throws Exception {
        TrackingRecordIterator iterator = new TrackingRecordIterator();
        CDCSourceSplitReader reader =
                createReader(new TestingTableRead(new SingleBatchRecordReader(iterator)));
        try {
            assignSplit(
                    reader,
                    new TableAwareFileStoreSourceSplit(
                            "id1",
                            new TestingSplit(),
                            1,
                            Identifier.create(DATABASE, TABLE),
                            1L,
                            1L));
            reader.wakeUp();

            RecordsWithSplitIds<RecordIterator<Event>> records = reader.fetch();
            assertThat(records.finishedSplits()).isEmpty();
            assertThat(records.nextSplit()).isNull();
        } finally {
            reader.close();
        }
        assertThat(iterator.released()).isTrue();
    }

    @Test
    public void testPauseOrResumeSplits() throws Exception {
        TestChangelogDataReadWrite rw = new TestChangelogDataReadWrite(tablePath);
        CDCSourceSplitReader reader = createReader(rw.createReadWithKey());

        List<Tuple2<Long, Long>> input1 = kvs();
        List<DataFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Long, Long>> input2 = kvs(6);
        List<DataFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        TableAwareFileStoreSourceSplit split1 = newSourceSplit("id1", row(1), 0, files);
        assignSplit(reader, split1);

        RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                0,
                input1.stream().map(t -> t.f1).collect(Collectors.toList()));

        // pause split1
        reader.pauseOrResumeSplits(Collections.singletonList(split1), Collections.emptyList());
        records = reader.fetch();
        assertRecords(records, null, null, 0, Collections.emptyList());

        // assign next split
        List<Tuple2<Long, Long>> input3 = kvs(12);
        List<DataFileMeta> files3 = rw.writeFiles(row(1), 0, input3);
        TableAwareFileStoreSourceSplit split2 = newSourceSplit("id2", row(1), 0, files3);
        assignSplit(reader, split2);

        records = reader.fetch();
        assertRecords(records, null, null, 0, Collections.emptyList());

        // resume split1
        reader.pauseOrResumeSplits(Collections.emptyList(), Collections.singletonList(split1));
        records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                6,
                input2.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        // fetch split2
        records = reader.fetch();
        assertRecords(
                records,
                null,
                "id2",
                0,
                input3.stream().map(t -> t.f1).collect(Collectors.toList()));
        records = reader.fetch();
        assertRecords(records, "id2", "id2", 0, null);

        reader.close();
    }

    private void assertRecords(
            RecordsWithSplitIds<BulkFormat.RecordIterator<Event>> records,
            String finishedSplit,
            String nextSplit,
            long startRecordSkipCount,
            List<Long> expected) {
        if (finishedSplit != null) {
            assertThat(records.finishedSplits()).isEqualTo(Collections.singleton(finishedSplit));
            return;
        }

        List<Tuple2<RowKind, Long>> result = readRecords(records, nextSplit, startRecordSkipCount);
        assertThat(result.stream().map(t -> t.f1).collect(Collectors.toList())).isEqualTo(expected);
    }

    private List<Tuple2<RowKind, Long>> readRecords(
            RecordsWithSplitIds<RecordIterator<Event>> records,
            String nextSplit,
            long startRecordSkipCount) {
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(records.nextSplit()).isEqualTo(nextSplit);
        List<Tuple2<RowKind, Long>> result = new ArrayList<>();
        RecordIterator<Event> iterator;
        while ((iterator = records.nextRecordFromSplit()) != null) {
            RecordAndPosition<Event> record;
            while ((record = iterator.next()) != null) {
                assertThat(record.getRecord()).isInstanceOf(DataChangeEvent.class);
                DataChangeEvent dataChangeEvent = (DataChangeEvent) record.getRecord();
                if (dataChangeEvent.op() == OperationType.INSERT) {
                    result.add(
                            new Tuple2<>(
                                    RowKind.INSERT,
                                    ((DataChangeEvent) record.getRecord()).after().getLong(0)));
                } else {
                    result.add(
                            new Tuple2<>(
                                    RowKind.DELETE,
                                    ((DataChangeEvent) record.getRecord()).before().getLong(0)));
                }
                assertThat(record.getRecordSkipCount()).isEqualTo(++startRecordSkipCount);
            }
        }
        records.recycle();
        return result;
    }

    private List<Class<?>> readEventTypes(
            RecordsWithSplitIds<RecordIterator<Event>> records, String nextSplit) {
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(records.nextSplit()).isEqualTo(nextSplit);
        List<Class<?>> result = new ArrayList<>();
        RecordIterator<Event> iterator;
        while ((iterator = records.nextRecordFromSplit()) != null) {
            RecordAndPosition<Event> record;
            while ((record = iterator.next()) != null) {
                result.add(
                        record.getRecord() instanceof SchemaChangeEvent
                                ? SchemaChangeEvent.class
                                : DataChangeEvent.class);
            }
        }
        records.recycle();
        return result;
    }

    private List<Long> readRecordSkipCounts(
            RecordsWithSplitIds<RecordIterator<Event>> records, String nextSplit) {
        assertThat(records.finishedSplits()).isEmpty();
        assertThat(records.nextSplit()).isEqualTo(nextSplit);
        List<Long> result = new ArrayList<>();
        RecordIterator<Event> iterator;
        while ((iterator = records.nextRecordFromSplit()) != null) {
            RecordAndPosition<Event> record;
            while ((record = iterator.next()) != null) {
                result.add(record.getRecordSkipCount());
            }
        }
        records.recycle();
        return result;
    }

    private List<SchemaChangeEvent> schemaChangeEvents() {
        return Collections.singletonList(
                new AddColumnEvent(
                        TableId.tableId(DATABASE, TABLE),
                        Arrays.asList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "extra",
                                                org.apache.flink.cdc.common.types.DataTypes
                                                        .BIGINT())))));
    }

    private List<SchemaChangeEvent> multipleSchemaChangeEvents() {
        return Arrays.asList(
                new AddColumnEvent(
                        TableId.tableId(DATABASE, TABLE),
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "extra_1",
                                                org.apache.flink.cdc.common.types.DataTypes
                                                        .BIGINT())))),
                new AddColumnEvent(
                        TableId.tableId(DATABASE, TABLE),
                        Collections.singletonList(
                                AddColumnEvent.last(
                                        Column.physicalColumn(
                                                "extra_2",
                                                org.apache.flink.cdc.common.types.DataTypes
                                                        .BIGINT())))));
    }

    private List<Tuple2<Long, Long>> kvs() {
        return kvs(0);
    }

    private List<Tuple2<Long, Long>> kvs(long keyBase) {
        List<Tuple2<Long, Long>> kvs = new ArrayList<>();
        kvs.add(new Tuple2<>(keyBase + 1L, 1L));
        kvs.add(new Tuple2<>(keyBase + 2L, 2L));
        kvs.add(new Tuple2<>(keyBase + 3L, 2L));
        kvs.add(new Tuple2<>(keyBase + 4L, -1L));
        kvs.add(new Tuple2<>(keyBase + 5L, 1L));
        kvs.add(new Tuple2<>(keyBase + 6L, -2L));
        return kvs;
    }

    private void assignSplit(CDCSourceSplitReader reader, TableAwareFileStoreSourceSplit split) {
        SplitsChange<TableAwareFileStoreSourceSplit> splitsChange =
                new SplitsAddition<>(Collections.singletonList(split));
        reader.handleSplitsChanges(splitsChange);
    }

    private RecordsWithSplitIds<RecordIterator<Event>> fetchWithoutWaitingForPool(
            CDCSourceSplitReader reader) throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<RecordsWithSplitIds<RecordIterator<Event>>> future =
                executorService.submit(reader::fetch);
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            reader.wakeUp();
            future.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Timed out waiting for split reader fetch.", e);
        } finally {
            executorService.shutdownNow();
        }
    }

    public static TableAwareFileStoreSourceSplit newSourceSplit(
            String id, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        return newSourceSplit(id, partition, bucket, files, false, 0);
    }

    public static TableAwareFileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental) {
        return newSourceSplit(id, partition, bucket, files, isIncremental, 0);
    }

    public static TableAwareFileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            long recordsToSkip) {
        return newSourceSplit(id, partition, bucket, files, false, recordsToSkip);
    }

    public static TableAwareFileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental,
            long recordsToSkip) {
        return newSourceSplit(id, partition, bucket, files, isIncremental, recordsToSkip, 0L);
    }

    public static TableAwareFileStoreSourceSplit newSourceSplit(
            String id,
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            boolean isIncremental,
            long recordsToSkip,
            long schemaChangeEventsToSkip) {
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withDataFiles(files)
                        .isStreaming(isIncremental)
                        .rawConvertible(false)
                        .withBucketPath("/temp/" + bucket) // no used
                        .build();
        return new TableAwareFileStoreSourceSplit(
                id,
                split,
                recordsToSkip,
                Identifier.create(DATABASE, TABLE),
                1L,
                1L,
                schemaChangeEventsToSkip);
    }

    private TableAwareFileStoreSourceSplit legacySplit(TableAwareFileStoreSourceSplit split)
            throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(split.splitId());
        InstantiationUtil.serializeObject(view, split.split());
        view.writeLong(split.recordsToSkip());
        view.writeUTF(JsonSerdeUtil.toJson(split.getIdentifier()));
        view.writeLong(split.getLastSchemaId() == null ? -1L : split.getLastSchemaId());
        view.writeLong(split.getSchemaId());
        return new TableAwareFileStoreSourceSplit.Serializer().deserialize(1, out.toByteArray());
    }

    private static class TestCDCSourceSplitReader extends CDCSourceSplitReader {
        private final TableRead tableRead;

        public TestCDCSourceSplitReader(
                FileStoreSourceReaderMetrics metrics,
                TableRead tableRead,
                List<SchemaChangeEvent> schemaChangeEvents) {
            super(metrics, new TestTableManager(tableRead, schemaChangeEvents));
            this.tableRead = tableRead;
        }

        @Override
        protected LazyRecordReader createLazyRecordReader(Split split) {
            return new TestLazyRecordReader(split, tableRead);
        }
    }

    private static class TestLazyRecordReader extends CDCSourceSplitReader.LazyRecordReader {
        private final TableRead tableRead;
        private RecordReader<InternalRow> lazyRecordReader;

        protected TestLazyRecordReader(Split split, TableRead tableRead) {
            super(split, null, new TestTableManager(tableRead));
            this.tableRead = tableRead;
        }

        @Override
        public RecordReader<InternalRow> recordReader() throws IOException {
            if (lazyRecordReader == null) {
                lazyRecordReader = tableRead.createReader(split);
            }
            return lazyRecordReader;
        }
    }

    private static class TestTableManager extends CDCSource.TableManager {
        private final TableRead tableRead;
        private final List<SchemaChangeEvent> schemaChangeEvents;

        public TestTableManager(TableRead tableRead) {
            this(tableRead, Collections.emptyList());
        }

        public TestTableManager(TableRead tableRead, List<SchemaChangeEvent> schemaChangeEvents) {
            super(null, null, null);
            this.tableRead = tableRead;
            this.schemaChangeEvents = schemaChangeEvents;
        }

        @Override
        public @Nullable TableSchema getTableSchema(
                Identifier identifier, @Nullable Long schemaId) {
            return TableSchema.create(1L, SCHEMA);
        }

        @Override
        public TableRead getTableRead(Identifier identifier, TableSchema schema) {
            return tableRead;
        }

        @Override
        public List<SchemaChangeEvent> generateSchemaChangeEventList(
                Identifier identifier, @Nullable Long lastSchemaId, long schemaId) {
            return schemaChangeEvents;
        }
    }

    private static class TestingTableRead implements TableRead {

        private final RecordReader<InternalRow> recordReader;

        private TestingTableRead(RecordReader<InternalRow> recordReader) {
            this.recordReader = recordReader;
        }

        @Override
        public TableRead withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public TableRead executeFilter() {
            return this;
        }

        @Override
        public TableRead withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public RecordReader<InternalRow> createReader(Split split) {
            return recordReader;
        }
    }

    private static class SingleBatchRecordReader implements RecordReader<InternalRow> {

        private final RecordReader.RecordIterator<InternalRow> iterator;
        private boolean returned;

        private SingleBatchRecordReader(RecordReader.RecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        @Nullable
        @Override
        public RecordReader.RecordIterator<InternalRow> readBatch() {
            if (returned) {
                return null;
            }

            returned = true;
            return iterator;
        }

        @Override
        public void close() {}
    }

    private static class FailingReleaseIterator
            implements RecordReader.RecordIterator<InternalRow> {

        @Nullable
        @Override
        public InternalRow next() {
            return null;
        }

        @Override
        public void releaseBatch() {
            throw new RuntimeException("release failed");
        }
    }

    private static class TrackingRecordIterator
            implements RecordReader.RecordIterator<InternalRow> {

        private boolean returned;
        private boolean released;

        @Nullable
        @Override
        public InternalRow next() {
            if (returned) {
                return null;
            }

            returned = true;
            return GenericRow.of(1L);
        }

        @Override
        public void releaseBatch() {
            released = true;
        }

        private boolean released() {
            return released;
        }
    }

    private static class TestingSplit implements Split {

        @Override
        public long rowCount() {
            return 0;
        }

        @Override
        public OptionalLong mergedRowCount() {
            return OptionalLong.empty();
        }
    }
}
