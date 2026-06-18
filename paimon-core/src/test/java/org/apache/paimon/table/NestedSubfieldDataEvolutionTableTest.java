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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for <b>sub-field-level</b> data evolution + row-tracking: updating a single sub-field of a
 * nested ROW column by writing an incremental file that only contains that sub-field, aligned by
 * row-id, and reading the struct back by assembling sub-fields from several files.
 */
public class NestedSubfieldDataEvolutionTableTest extends DataEvolutionTestBase {

    // f0(0) INT, f1(1) STRING, nest(2) ROW<a INT, b STRING>, arr(3) ARRAY<INT>, mp(4)
    // MAP<STRING,INT>
    @Override
    protected Schema schemaDefault() {
        Schema.Builder b = Schema.newBuilder();
        b.column("f0", DataTypes.INT());
        b.column("f1", DataTypes.STRING());
        b.column(
                "nest",
                DataTypes.ROW(
                        DataTypes.FIELD(0, "a", DataTypes.INT()),
                        DataTypes.FIELD(1, "b", DataTypes.STRING())));
        b.column("arr", DataTypes.ARRAY(DataTypes.INT()));
        b.column("mp", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        b.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        b.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return b.build();
    }

    private static GenericArray arrOf(int v) {
        return new GenericArray(new int[] {v});
    }

    private static GenericMap mapOf(String k, int v) {
        Map<Object, Object> m = new HashMap<>();
        m.put(BinaryString.fromString(k), v);
        return new GenericMap(m);
    }

    private void commit(BatchWriteBuilder builder, List<CommitMessage> messages) throws Exception {
        try (BatchTableCommit commit = builder.newCommit()) {
            commit.commit(messages);
        }
    }

    /**
     * Core: write sub-field {@code nest.a} and {@code nest.b} into two separate files (plus the
     * rest of the columns in a third), then read the full struct assembled from all three.
     */
    @Test
    public void testSubFieldGroupsAssembled() throws Exception {
        createTableDefault();
        RowType full = getTableDefault().rowType();
        int n = 40;

        RowType cgRest = full.projectByPaths(Arrays.asList("f0", "f1", "arr", "mp"));
        RowType cgA = full.projectByPaths(Collections.singletonList("nest.a"));
        RowType cgB = full.projectByPaths(Collections.singletonList("nest.b"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite w = builder.newWrite().withWriteType(cgRest)) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i, BinaryString.fromString("a" + i), arrOf(i), mapOf("k" + i, i)));
            }
            commit(builder, w.prepareCommit());
        }
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgA)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(GenericRow.of(i * 2)));
            }
            List<CommitMessage> messages = w.prepareCommit();
            // a file that only writes nest.a must record a nested dotted path
            assertThat(((CommitMessageImpl) messages.get(0)).newFilesIncrement().newFiles())
                    .allSatisfy(f -> assertThat(f.writeCols()).containsExactly("nest.a"));
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgB)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(GenericRow.of(BinaryString.fromString("b" + i))));
            }
            List<CommitMessage> messages = w.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    assertThat(r.getInt(0)).isEqualTo(i);
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i);
                    // nest assembled: a from file A, b from file B
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i * 2);
                    assertThat(nest.getString(1).toString()).isEqualTo("b" + i);
                    assertThat(r.getArray(3).getInt(0)).isEqualTo(i);
                    assertThat(r.getMap(4).valueArray().getInt(0)).isEqualTo(i);
                });
        assertThat(idx.get()).isEqualTo(n);

        assertThat(readRowIds()).containsExactlyElementsOf(rangeLongs(n));
    }

    /**
     * A later snapshot updates only {@code nest.a} via an incremental sub-field file; {@code
     * nest.b} and all other columns keep their original values, row-ids unchanged.
     */
    @Test
    public void testSubFieldLateOverwrite() throws Exception {
        createTableDefault();
        RowType full = getTableDefault().rowType();
        int n = 25;

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        // full write: nest = (i, "old"+i)
        try (BatchTableWrite w = builder.newWrite().withWriteType(full)) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                GenericRow.of(i, BinaryString.fromString("old" + i)),
                                arrOf(i),
                                mapOf("k" + i, i)));
            }
            commit(builder, w.prepareCommit());
        }

        // incremental: overwrite only nest.a
        RowType cgA = full.projectByPaths(Collections.singletonList("nest.a"));
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgA)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(GenericRow.of(i + 1000)));
            }
            List<CommitMessage> messages = w.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    assertThat(r.getInt(0)).isEqualTo(i);
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i + 1000); // updated sub-field
                    assertThat(nest.getString(1).toString()).isEqualTo("old" + i); // kept
                });
        assertThat(idx.get()).isEqualTo(n);
        assertThat(readRowIds()).containsExactlyElementsOf(rangeLongs(n));
    }

    /** Compaction merges sub-field files into one full file; values and row-ids are preserved. */
    @Test
    public void testCompactionMergesSubFields() throws Exception {
        createTableDefault();
        RowType full = getTableDefault().rowType();
        int n = 20;

        RowType cgRest = full.projectByPaths(Arrays.asList("f0", "f1", "arr", "mp"));
        RowType cgA = full.projectByPaths(Collections.singletonList("nest.a"));
        RowType cgB = full.projectByPaths(Collections.singletonList("nest.b"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite w = builder.newWrite().withWriteType(cgRest)) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i, BinaryString.fromString("a" + i), arrOf(i), mapOf("k" + i, i)));
            }
            commit(builder, w.prepareCommit());
        }
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgA)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(GenericRow.of(i * 2)));
            }
            List<CommitMessage> messages = w.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgB)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(GenericRow.of(BinaryString.fromString("b" + i))));
            }
            List<CommitMessage> messages = w.prepareCommit();
            setFirstRowId(messages, 0L);
            commit(builder, messages);
        }

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<CommitMessage> compactMessages = new ArrayList<>();
        List<DataEvolutionCompactTask> tasks;
        try {
            while (!(tasks = coordinator.plan()).isEmpty()) {
                for (DataEvolutionCompactTask task : tasks) {
                    compactMessages.add(task.doCompact(table, "subfield-compact"));
                }
            }
        } catch (EndOfScanException ignore) {
        }
        if (!compactMessages.isEmpty()) {
            commit(table.newBatchWriteBuilder(), compactMessages);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    assertThat(r.getInt(0)).isEqualTo(i);
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i * 2);
                    assertThat(nest.getString(1).toString()).isEqualTo("b" + i);
                });
        assertThat(idx.get()).isEqualTo(n);
        assertThat(readRowIds()).containsExactlyElementsOf(rangeLongs(n));
    }

    private List<Long> rangeLongs(int n) {
        List<Long> out = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            out.add(i);
        }
        return out;
    }

    private List<Long> readRowIds() throws Exception {
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder
                        .withReadType(RowType.of(SpecialFields.ROW_ID))
                        .newRead()
                        .createReader(readBuilder.newScan().plan());
        List<Long> rowIds = new ArrayList<>();
        reader.forEachRemaining(r -> rowIds.add(r.getLong(0)));
        return rowIds;
    }
}
