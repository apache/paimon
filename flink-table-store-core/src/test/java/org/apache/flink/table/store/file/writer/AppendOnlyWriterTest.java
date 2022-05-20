/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the correctness for {@link AppendOnlyWriter}. */
public class AppendOnlyWriterTest {

    private static final RowData EMPTY_ROW = BinaryRowDataUtil.EMPTY_ROW;
    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});
    private static final FieldStatsArraySerializer STATS_SERIALIZER =
            new FieldStatsArraySerializer(SCHEMA);

    @TempDir public java.nio.file.Path tempDir;
    public DataFilePathFactory pathFactory;

    private static final String AVRO = "avro";
    private static final String PART = "2022-05-01";

    @BeforeEach
    public void before() {
        pathFactory = createPathFactory();
    }

    @Test
    public void testEmptyCommits() throws Exception {
        RecordWriter writer = createWriter(1024 * 1024L, SCHEMA, 0);

        for (int i = 0; i < 3; i++) {
            writer.sync();
            Increment inc = writer.prepareCommit();

            assertThat(inc.newFiles()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());
        }
    }

    @Test
    public void testSingleWrite() throws Exception {
        RecordWriter writer = createWriter(1024 * 1024L, SCHEMA, 0);
        writer.write(ValueKind.ADD, EMPTY_ROW, row(1, "AAA", PART));

        List<DataFileMeta> result = writer.close();

        assertThat(result.size()).isEqualTo(1);
        DataFileMeta meta = result.get(0);
        assertThat(meta).isNotNull();

        Path path = pathFactory.toPath(meta.fileName());
        assertThat(path.getFileSystem().exists(path)).isFalse();

        assertThat(meta.rowCount()).isEqualTo(1L);
        assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

        FieldStats[] expected =
                new FieldStats[] {
                    initStats(1, 1, 0), initStats("AAA", "AAA", 0), initStats(PART, PART, 0)
                };
        assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

        assertThat(meta.minSequenceNumber()).isEqualTo(1);
        assertThat(meta.maxSequenceNumber()).isEqualTo(1);
        assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
    }

    @Test
    public void testMultipleCommits() throws Exception {
        RecordWriter writer = createWriter(1024 * 1024L, SCHEMA, 0);

        // Commit 5 continues txn.
        for (int txn = 0; txn < 5; txn += 1) {

            // Write the records with range [ txn*100, (txn+1)*100 ).
            int start = txn * 100;
            int end = txn * 100 + 100;
            for (int i = start; i < end; i++) {
                writer.write(ValueKind.ADD, EMPTY_ROW, row(i, String.format("%03d", i), PART));
            }

            writer.sync();
            Increment inc = writer.prepareCommit();
            assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
            assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());

            assertThat(inc.newFiles().size()).isEqualTo(1);
            DataFileMeta meta = inc.newFiles().get(0);

            Path path = pathFactory.toPath(meta.fileName());
            assertThat(path.getFileSystem().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(100L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(start, end - 1, 0),
                        initStats(String.format("%03d", start), String.format("%03d", end - 1), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(start + 1);
            assertThat(meta.maxSequenceNumber()).isEqualTo(end);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
        }
    }

    @Test
    public void testRollingWrite() throws Exception {
        // Set a very small target file size, so that we will roll over to a new file even if
        // writing one record.
        RecordWriter writer = createWriter(10L, SCHEMA, 0);

        for (int i = 0; i < 10; i++) {
            writer.write(ValueKind.ADD, EMPTY_ROW, row(i, String.format("%03d", i), PART));
        }

        writer.sync();
        Increment inc = writer.prepareCommit();
        assertThat(inc.compactBefore()).isEqualTo(Collections.emptyList());
        assertThat(inc.compactAfter()).isEqualTo(Collections.emptyList());

        assertThat(inc.newFiles().size()).isEqualTo(10);

        int id = 0;
        for (DataFileMeta meta : inc.newFiles()) {
            Path path = pathFactory.toPath(meta.fileName());
            assertThat(path.getFileSystem().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(1L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(id, id, 0),
                        initStats(String.format("%03d", id), String.format("%03d", id), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(id + 1);
            assertThat(meta.maxSequenceNumber()).isEqualTo(id + 1);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);

            id += 1;
        }
    }

    private FieldStats initStats(Integer min, Integer max, long nullCount) {
        return new FieldStats(min, max, nullCount);
    }

    private FieldStats initStats(String min, String max, long nullCount) {
        return new FieldStats(StringData.fromString(min), StringData.fromString(max), nullCount);
    }

    private RowData row(int id, String name, String dt) {
        return GenericRowData.of(id, StringData.fromString(name), StringData.fromString(dt));
    }

    private DataFilePathFactory createPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir.toString()),
                "dt=" + PART,
                1,
                FileStoreOptions.FILE_FORMAT.defaultValue());
    }

    private RecordWriter createWriter(long targetFileSize, RowType writeSchema, long maxSeqNum) {
        FileFormat fileFormat =
                FileFormat.fromIdentifier(
                        Thread.currentThread().getContextClassLoader(), AVRO, new Configuration());

        return new AppendOnlyWriter(
                fileFormat, targetFileSize, writeSchema, maxSeqNum, pathFactory);
    }
}
