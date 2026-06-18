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
import org.apache.paimon.reader.DataEvolutionFileReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for data-evolution + row-tracking with nested columns (ROW / ARRAY / MAP).
 *
 * <p>These tests verify that a nested column is handled as a single, indivisible top-level field by
 * data evolution: it can live in its own column-group file and be merged back by field id, while
 * {@code _ROW_ID} / {@code _SEQUENCE_NUMBER} stay aligned by {@code firstRowId + position}. They
 * also pin down the limitation that sub-fields of a nested ROW cannot be split into a separate
 * column group (the minimum granularity is a top-level column).
 */
public class NestedDataEvolutionTableTest extends DataEvolutionTestBase {

    // f0(0) INT, f1(1) STRING, nest(2) ROW<a INT, b STRING>, arr(3) ARRAY<INT>, mp(4)
    // MAP<STRING,INT>
    @Override
    protected Schema schemaDefault() {
        Schema.Builder b = Schema.newBuilder();
        b.column("f0", DataTypes.INT());
        b.column("f1", DataTypes.STRING());
        b.column("nest", DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
        b.column("arr", DataTypes.ARRAY(DataTypes.INT()));
        b.column("mp", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
        b.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        b.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return b.build();
    }

    private static GenericRow nestOf(int a, String bStr) {
        return GenericRow.of(a, BinaryString.fromString(bStr));
    }

    private static GenericArray arrOf(int... values) {
        return new GenericArray(values);
    }

    private static GenericMap mapOf(String k, int v) {
        Map<Object, Object> m = new HashMap<>();
        m.put(BinaryString.fromString(k), v);
        return new GenericMap(m);
    }

    /** G1 + G2: nested column as its own column group, merged by field id, row-id aligned. */
    @Test
    public void testNestedColumnGroupMerge() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        int n = 50;

        RowType cgA = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType cgB = schema.rowType().project(Collections.singletonList("nest"));
        RowType cgC = schema.rowType().project(Arrays.asList("arr", "mp"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        // column group A: {f0, f1}
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgA)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(w.prepareCommit());
        }

        // column group B: {nest}
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgB)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(nestOf(i, "n" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = w.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        // column group C: {arr, mp}
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgC)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(arrOf(i, i + 1), mapOf("k" + i, i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = w.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        assertThat(reader).isInstanceOf(DataEvolutionFileReader.class);

        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    assertThat(r.getInt(0)).isEqualTo(i);
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i);
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i);
                    assertThat(nest.getString(1).toString()).isEqualTo("n" + i);
                    assertThat(r.getArray(3).getInt(0)).isEqualTo(i);
                    assertThat(r.getArray(3).getInt(1)).isEqualTo(i + 1);
                    assertThat(r.getMap(4).keyArray().getString(0).toString()).isEqualTo("k" + i);
                    assertThat(r.getMap(4).valueArray().getInt(0)).isEqualTo(i);
                });
        assertThat(idx.get()).isEqualTo(n);

        // _ROW_ID is contiguous 0..n-1 across the merged column groups.
        List<Long> rowIds = readRowIds();
        assertThat(rowIds).hasSize(n);
        for (int i = 0; i < n; i++) {
            assertThat(rowIds.get(i)).isEqualTo((long) i);
        }
    }

    /**
     * G3: a later snapshot rewrites only the nested column group; other columns keep old values.
     */
    @Test
    public void testNestedColumnLateOverwrite() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        int n = 20;

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        // full write
        try (BatchTableWrite w = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                nestOf(i, "old" + i),
                                arrOf(i),
                                mapOf("k" + i, i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(w.prepareCommit());
        }

        // later snapshot: overwrite only the nest column group, aligned to the same row-id range
        RowType cgNest = schema.rowType().project(Collections.singletonList("nest"));
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgNest)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(nestOf(i * 10, "new" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = w.prepareCommit();
            setFirstRowId(commitables, 0L);
            commit.commit(commitables);
        }

        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    // untouched columns keep old values
                    assertThat(r.getInt(0)).isEqualTo(i);
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i);
                    assertThat(r.getArray(3).getInt(0)).isEqualTo(i);
                    // nest column reflects the new values
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i * 10);
                    assertThat(nest.getString(1).toString()).isEqualTo("new" + i);
                });
        assertThat(idx.get()).isEqualTo(n);

        // row ids unchanged
        List<Long> rowIds = readRowIds();
        for (int i = 0; i < n; i++) {
            assertThat(rowIds.get(i)).isEqualTo((long) i);
        }
    }

    /** G4: projection covering special fields and nested columns. */
    @Test
    public void testProjectionWithNested() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        int n = 10;

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite w = builder.newWrite().withWriteType(schema.rowType())) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("a" + i),
                                nestOf(i, "n" + i),
                                arrOf(i),
                                mapOf("k" + i, i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(w.prepareCommit());
        }

        // project only the nested column (keep the real "nest" field id via project)
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder
                        .withReadType(
                                getTableDefault()
                                        .rowType()
                                        .project(Collections.singletonList("nest")))
                        .newRead()
                        .createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    InternalRow nest = r.getRow(0, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i);
                    assertThat(nest.getString(1).toString()).isEqualTo("n" + i);
                });
        assertThat(idx.get()).isEqualTo(n);

        // project only _ROW_ID
        assertThat(readRowIds()).hasSize(n);
    }

    /**
     * G5 (limitation): a sub-field of a nested ROW cannot be addressed as a top-level write column,
     * because data evolution splits at top-level column granularity. {@code RowType.project}
     * matches only top-level field names, so a nested sub-field name has no match.
     */
    @Test
    public void testNestedSubFieldCannotBeSplit() {
        RowType rowType = schemaDefault().rowType();
        // "a" / "b" are sub-fields inside "nest"; they are not top-level columns.
        assertThatThrownBy(() -> rowType.project(Collections.singletonList("a")))
                .isInstanceOf(IndexOutOfBoundsException.class);
        // a nested ROW can only be projected as a whole top-level column.
        assertThat(rowType.project(Collections.singletonList("nest")).getFieldNames())
                .containsExactly("nest");
    }

    /**
     * G6: compacting column-group files (one of which is the nested column) merges them into a
     * single file; data values and row ids survive the {@code DataEvolutionRowIdReassigner}.
     */
    @Test
    public void testCompactionWithNested() throws Exception {
        createTableDefault();
        Schema schema = schemaDefault();
        int n = 30;

        RowType cgA = schema.rowType().project(Arrays.asList("f0", "f1", "arr", "mp"));
        RowType cgB = schema.rowType().project(Collections.singletonList("nest"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();

        try (BatchTableWrite w = builder.newWrite().withWriteType(cgA)) {
            for (int i = 0; i < n; i++) {
                w.write(
                        GenericRow.of(
                                i, BinaryString.fromString("a" + i), arrOf(i), mapOf("k" + i, i)));
            }
            builder.newCommit().commit(w.prepareCommit());
        }
        try (BatchTableWrite w = builder.newWrite().withWriteType(cgB)) {
            for (int i = 0; i < n; i++) {
                w.write(GenericRow.of(nestOf(i, "n" + i)));
            }
            List<CommitMessage> commitables = w.prepareCommit();
            setFirstRowId(commitables, 0L);
            builder.newCommit().commit(commitables);
        }

        // run data-evolution compaction via the coordinator (merges the two column groups)
        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false);
        List<CommitMessage> commitMessages = new ArrayList<>();
        List<DataEvolutionCompactTask> tasks;
        try {
            while (!(tasks = coordinator.plan()).isEmpty()) {
                for (DataEvolutionCompactTask task : tasks) {
                    commitMessages.add(task.doCompact(table, "nested-compact"));
                }
            }
        } catch (EndOfScanException ignore) {
        }
        if (!commitMessages.isEmpty()) {
            table.newBatchWriteBuilder().newCommit().commit(commitMessages);
        }

        // data and row ids must be unchanged after compaction
        ReadBuilder readBuilder = getTableDefault().newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        AtomicInteger idx = new AtomicInteger(0);
        reader.forEachRemaining(
                r -> {
                    int i = idx.getAndIncrement();
                    assertThat(r.getInt(0)).isEqualTo(i);
                    assertThat(r.getString(1).toString()).isEqualTo("a" + i);
                    InternalRow nest = r.getRow(2, 2);
                    assertThat(nest.getInt(0)).isEqualTo(i);
                    assertThat(nest.getString(1).toString()).isEqualTo("n" + i);
                    assertThat(r.getArray(3).getInt(0)).isEqualTo(i);
                });
        assertThat(idx.get()).isEqualTo(n);

        List<Long> rowIds = readRowIds();
        assertThat(rowIds).hasSize(n);
        for (int i = 0; i < n; i++) {
            assertThat(rowIds.get(i)).isEqualTo((long) i);
        }
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
