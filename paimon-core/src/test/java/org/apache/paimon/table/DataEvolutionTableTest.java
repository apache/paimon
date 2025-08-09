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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for table with data evolution. */
public class DataEvolutionTableTest extends TableTestBase {

    @Test
    public void testBasic() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            write.write(
                    GenericRow.of(1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write.prepareCommit();
            commit.commit(commitables);
        }

        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2).toString()).isEqualTo("c");
                });
    }

    @Test
    public void testMultipleAppends() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();

        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite write = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < 100; i++) {
                write.write(
                        GenericRow.of(
                                1, BinaryString.fromString("a"), BinaryString.fromString("b")));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write.prepareCommit();
            commit.commit(commitables);
        }

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write1.write(GenericRow.of(BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 100L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(2, BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 101L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 101L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<InternalRow> readRows = new ArrayList<>();
        reader.forEachRemaining(readRows::add);

        assertThat(readRows.size()).isEqualTo(102);

        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    if (i.get() < 101) {
                        assertThat(r.getInt(0)).isEqualTo(1);
                        assertThat(r.getString(1).toString()).isEqualTo("a");
                        assertThat(r.getString(2).toString()).isEqualTo("b");
                    } else {
                        assertThat(r.getInt(0)).isEqualTo(2);
                        assertThat(r.getString(1).toString()).isEqualTo("c");
                        assertThat(r.getString(2).toString()).isEqualTo("d"); // "d" from the append
                    }
                    i.incrementAndGet();
                });
    }

    @Test
    public void testOnlySomeColumns() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Collections.singletonList("f0"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f1"));
        RowType writeType2 = schema.rowType().project(Collections.singletonList("f2"));

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Commit 1: f0
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(1));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            commit.commit(commitables);
        }

        // Commit 2: f1
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("a")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Commit 3: f2
        try (BatchTableWrite write2 = builder.newWrite().withWriteType(writeType2)) {
            write2.write(GenericRow.of(BinaryString.fromString("b")));
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write2.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);

        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.getString(1).toString()).isEqualTo("a");
                    assertThat(r.getString(2)).isEqualTo(BinaryString.fromString("b"));
                });
    }

    @Test
    public void testNullValues() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // Commit 1: Some values are null
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, null));
            write1.write(new GenericRow(1));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Commit 2: Overwrite with non-null
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(1);
                    assertThat(r.isNullAt(1)).isTrue();
                    assertThat(r.getString(2).toString()).isEqualTo("c");
                });
    }

    @Test
    public void testMultipleAppendsDifferentFirstRowIds() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // First commit, firstRowId = 0
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            write0.write(GenericRow.of(1, BinaryString.fromString("a")));
            write1.write(GenericRow.of(BinaryString.fromString("b")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // Second commit, firstRowId = 1
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            write0.write(GenericRow.of(2, BinaryString.fromString("c")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write0.prepareCommit();
            setFirstRowId(commitables, 1L); // Different firstRowId
            commit.commit(commitables);
        }

        // Third commit
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            write1.write(GenericRow.of(BinaryString.fromString("d")));

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, 1L); // Different firstRowId
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());

        List<InternalRow> readRows = new ArrayList<>();
        reader.forEachRemaining(readRows::add);

        assertThat(readRows.size()).isEqualTo(2);

        assertThat(readRows.get(0).getInt(0)).isEqualTo(1);
        assertThat(readRows.get(0).getString(1).toString()).isEqualTo("a");
        assertThat(readRows.get(0).getString(2).toString()).isEqualTo("b");

        assertThat(readRows.get(1).getInt(0)).isEqualTo(2);
        assertThat(readRows.get(1).getString(1).toString()).isEqualTo("c");
        assertThat(readRows.get(1).getString(2).toString()).isEqualTo("d");
    }

    private void setFirstRowId(List<CommitMessage> commitables, long firstRowId) {
        commitables.forEach(
                c -> {
                    CommitMessageImpl commitMessage = (CommitMessageImpl) c;
                    List<DataFileMeta> newFiles =
                            new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
                    commitMessage.newFilesIncrement().newFiles().clear();
                    commitMessage
                            .newFilesIncrement()
                            .newFiles()
                            .addAll(
                                    newFiles.stream()
                                            .map(s -> s.assignFirstRowId(firstRowId))
                                            .collect(Collectors.toList()));
                });
    }

    @Test
    public void testMoreData() throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0);
                BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {

            for (int i = 0; i < 10000; i++) {
                write0.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
                write1.write(GenericRow.of(BinaryString.fromString("b" + i)));
            }

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = new ArrayList<>();
            commitables.addAll(write0.prepareCommit());
            commitables.addAll(write1.prepareCommit());
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            for (int i = 0; i < 10000; i++) {
                write1.write(GenericRow.of(BinaryString.fromString("c" + i)));
            }

            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);
        AtomicInteger i = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    assertThat(r.getInt(0)).isEqualTo(i.get());
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i.get());
                    assertThat(r.getString(2).toString()).isEqualTo("c" + i.get());
                    i.incrementAndGet();
                });
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }
}
