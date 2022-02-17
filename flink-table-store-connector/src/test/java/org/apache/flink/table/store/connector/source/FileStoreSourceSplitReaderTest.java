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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FileStoreSourceSplitReader}. */
public class FileStoreSourceSplitReaderTest {

    private static ExecutorService service;

    @TempDir java.nio.file.Path tempDir;

    private final AtomicInteger v = new AtomicInteger(0);

    @BeforeAll
    public static void before() {
        service = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void after() {
        service.shutdownNow();
        service = null;
    }

    @Test
    public void testKeyAsRecord() throws Exception {
        innerTestOnce(true);
    }

    @Test
    public void testNonKeyAsRecord() throws Exception {
        innerTestOnce(false);
    }

    private void innerTestOnce(boolean keyAsRecord) throws Exception {
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader =
                new FileStoreSourceSplitReader(rw.createRead(), keyAsRecord);

        List<Tuple2<Integer, Integer>> input = kvs();
        List<SstFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, new FileStoreSourceSplit("id1", row(1), 0, files));

        RecordsWithSplitIds<RecordAndPosition<RowData>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                0,
                input.stream().map(t -> keyAsRecord ? t.f0 : t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testMultipleBatchInSplit() throws Exception {
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader = new FileStoreSourceSplitReader(rw.createRead(), false);

        List<Tuple2<Integer, Integer>> input1 = kvs();
        List<SstFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Integer, Integer>> input2 = kvs();
        List<SstFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, new FileStoreSourceSplit("id1", row(1), 0, files));

        RecordsWithSplitIds<RecordAndPosition<RowData>> records = reader.fetch();
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
                5,
                input2.stream().map(t -> t.f1).collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testRestore() throws Exception {
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader = new FileStoreSourceSplitReader(rw.createRead(), false);

        List<Tuple2<Integer, Integer>> input = kvs();
        List<SstFileMeta> files = rw.writeFiles(row(1), 0, input);

        assignSplit(reader, new FileStoreSourceSplit("id1", row(1), 0, files, 3));

        RecordsWithSplitIds<RecordAndPosition<RowData>> records = reader.fetch();
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
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader = new FileStoreSourceSplitReader(rw.createRead(), false);

        List<Tuple2<Integer, Integer>> input1 = kvs();
        List<SstFileMeta> files = rw.writeFiles(row(1), 0, input1);

        List<Tuple2<Integer, Integer>> input2 = kvs();
        List<SstFileMeta> files2 = rw.writeFiles(row(1), 0, input2);
        files.addAll(files2);

        assignSplit(reader, new FileStoreSourceSplit("id1", row(1), 0, files, 7));

        RecordsWithSplitIds<RecordAndPosition<RowData>> records = reader.fetch();
        assertRecords(
                records,
                null,
                "id1",
                7,
                input2.subList(2, input2.size()).stream()
                        .map(t -> t.f1)
                        .collect(Collectors.toList()));

        records = reader.fetch();
        assertRecords(records, "id1", "id1", 0, null);

        reader.close();
    }

    @Test
    public void testMultipleSplits() throws Exception {
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader = new FileStoreSourceSplitReader(rw.createRead(), false);

        List<Tuple2<Integer, Integer>> input1 = kvs();
        List<SstFileMeta> files1 = rw.writeFiles(row(1), 0, input1);
        assignSplit(reader, new FileStoreSourceSplit("id1", row(1), 0, files1));

        List<Tuple2<Integer, Integer>> input2 = kvs();
        List<SstFileMeta> files2 = rw.writeFiles(row(2), 1, input2);
        assignSplit(reader, new FileStoreSourceSplit("id2", row(2), 1, files2));

        RecordsWithSplitIds<RecordAndPosition<RowData>> records = reader.fetch();
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
        TestDataReadWrite rw = new TestDataReadWrite(tempDir.toString(), service);
        FileStoreSourceSplitReader reader = new FileStoreSourceSplitReader(rw.createRead(), false);
        assertThatThrownBy(reader::fetch).hasMessageContaining("no split remaining");
        reader.close();
    }

    private void assertRecords(
            RecordsWithSplitIds<RecordAndPosition<RowData>> records,
            String finishedSplit,
            String nextSplit,
            long startRecordSkipCount,
            List<Integer> expected) {
        if (finishedSplit != null) {
            assertThat(records.finishedSplits()).isEqualTo(Collections.singleton(finishedSplit));
            return;
        } else {
            assertThat(records.finishedSplits()).isEmpty();
        }

        assertThat(records.nextSplit()).isEqualTo(nextSplit);

        List<Integer> result = new ArrayList<>();
        RecordAndPosition<RowData> record;
        while ((record = records.nextRecordFromSplit()) != null) {
            result.add(record.getRecord().getInt(0));
            assertThat(record.getRecordSkipCount()).isEqualTo(++startRecordSkipCount);
        }
        records.recycle();

        assertThat(result).isEqualTo(expected);
    }

    private List<Tuple2<Integer, Integer>> kvs() {
        List<Tuple2<Integer, Integer>> kvs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            kvs.add(new Tuple2<>(next(), next()));
        }
        return kvs;
    }

    private int next() {
        return v.incrementAndGet();
    }

    private void assignSplit(FileStoreSourceSplitReader reader, FileStoreSourceSplit split) {
        SplitsChange<FileStoreSourceSplit> splitsChange =
                new SplitsAddition<>(Collections.singletonList(split));
        reader.handleSplitsChanges(splitsChange);
    }
}
