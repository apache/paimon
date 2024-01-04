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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.flink.lookup.LookupTable.TableBulkLoader;
import org.apache.paimon.lookup.BulkLoader;
import org.apache.paimon.lookup.RocksDBStateFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SortUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.singletonList;
import static org.apache.paimon.schema.SystemColumns.SEQUENCE_NUMBER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LookupTable}. */
public class LookupTableTest {

    @TempDir Path tempDir;

    private RocksDBStateFactory stateFactory;

    private RowType rowType;

    @BeforeEach
    public void before() throws IOException {
        this.stateFactory = new RocksDBStateFactory(tempDir.toString(), new Options(), null);
        this.rowType = RowType.of(new IntType(), new IntType(), new IntType());
    }

    @AfterEach
    public void after() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
        }
    }

    @Test
    public void testPkTable() throws IOException, BulkLoader.WriteException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> true,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        // test bulk load error
        {
            TableBulkLoader bulkLoader = table.createBulkLoader();
            bulkLoader.write(new byte[] {1}, new byte[] {1});
            assertThatThrownBy(() -> bulkLoader.write(new byte[] {1}, new byte[] {2}))
                    .hasMessageContaining("Keys must be added in strict ascending order");
        }

        // test bulk load 100_000 records
        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 1; i <= 100_000; i++) {
            InternalRow row = row(i, 11 * i, 111 * i);
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(sequence(row, -1L))));
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        for (int i = 1; i <= 100_000; i++) {
            List<InternalRow> result = table.get(row(i));
            assertThat(result).hasSize(1);
            assertRow(result.get(0), i, 11 * i, 111 * i);
        }

        // test refresh to update
        table.refresh(singletonList(sequence(row(1, 22, 222), -1L)).iterator(), false);
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // test refresh to delete
        table.refresh(
                singletonList(sequence(row(RowKind.DELETE, 1, 11, 111), -1L)).iterator(), false);
        assertThat(table.get(row(1))).hasSize(0);

        table.refresh(
                singletonList(sequence(row(RowKind.DELETE, 3, 33, 333), -1L)).iterator(), false);
        assertThat(table.get(row(3))).hasSize(0);
    }

    @Test
    public void testPkTableWithSequenceField() throws Exception {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType.appendDataField(SEQUENCE_NUMBER, DataTypes.BIGINT()),
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> true,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            InternalRow row = sequence(row(i, 11 * i, 111 * i), -1L);
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(row)));
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        // test refresh to update
        table.refresh(singletonList(sequence(row(1, 22, 222), 1L)).iterator(), true);
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // refresh with old sequence
        table.refresh(singletonList((sequence(row(1, 33, 333), 0L))).iterator(), true);
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // test refresh delete data with old sequence
        table.refresh(
                singletonList(sequence(row(RowKind.DELETE, 1, 11, 111), -1L)).iterator(), true);
        assertThat(table.get(row(1))).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);
    }

    @Test
    public void testPkTablePkFilter() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> r.getInt(0) < 3,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(sequence(row(1, 11, 111), -1L)).iterator(), false);
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(sequence(row(1, 22, 222), -1L)).iterator(), false);
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(
                singletonList(sequence(row(RowKind.DELETE, 1, 11, 111), -1L)).iterator(), false);
        assertThat(table.get(row(1))).hasSize(0);

        table.refresh(singletonList(sequence(row(3, 33, 333), -1L)).iterator(), false);
        assertThat(table.get(row(3))).hasSize(0);
    }

    @Test
    public void testPkTableNonPkFilter() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f0"),
                        r -> r.getInt(1) < 22,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(sequence(row(1, 11, 111), -1L)).iterator(), false);
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(sequence(row(1, 22, 222), -1L)).iterator(), false);
        result = table.get(row(1));
        assertThat(result).hasSize(0);
    }

    @Test
    public void testSecKeyTable() throws IOException, BulkLoader.WriteException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f1"),
                        r -> true,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        // test bulk load 100_000 records
        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        Random rnd = new Random();
        Map<Integer, Set<Integer>> secKeyToPk = new HashMap<>();
        for (int i = 1; i <= 100_000; i++) {
            int secKey = rnd.nextInt(i);
            InternalRow row = row(i, secKey, 111 * i);
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(sequence(row, -1L))));
            secKeyToPk.computeIfAbsent(secKey, k -> new HashSet<>()).add(i);
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        for (Map.Entry<Integer, Set<Integer>> entry : secKeyToPk.entrySet()) {
            List<InternalRow> result = table.get(row(entry.getKey()));
            assertThat(result.stream().map(row -> row.getInt(0)))
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }

        // add new sec key to pk
        table.refresh(singletonList(sequence(row(1, 22, 222), -1L)).iterator(), false);
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);
    }

    @Test
    public void testSecKeyTableWithSequenceField() throws Exception {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType.appendDataField(SEQUENCE_NUMBER, DataTypes.BIGINT()),
                        singletonList("f0"),
                        singletonList("f1"),
                        r -> true,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        Random rnd = new Random();
        Map<Integer, Set<Integer>> secKeyToPk = new HashMap<>();
        for (int i = 1; i <= 10; i++) {
            int secKey = rnd.nextInt(i);
            InternalRow row = new JoinedRow(row(i, secKey, 111 * i), GenericRow.of(-1L));
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(row)));
            secKeyToPk.computeIfAbsent(secKey, k -> new HashSet<>()).add(i);
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        for (Map.Entry<Integer, Set<Integer>> entry : secKeyToPk.entrySet()) {
            List<InternalRow> result = table.get(row(entry.getKey()));
            assertThat(result.stream().map(row -> row.getInt(0)))
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }

        JoinedRow joined = new JoinedRow();
        // add new sec key to pk
        table.refresh(
                singletonList((InternalRow) joined.replace(row(1, 22, 222), GenericRow.of(1L)))
                        .iterator(),
                true);
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);

        // refresh with old value
        table.refresh(
                singletonList((InternalRow) joined.replace(row(1, 22, 333), GenericRow.of(0L)))
                        .iterator(),
                true);
        result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(2))).doesNotContain(333);
    }

    @Test
    public void testSecKeyTablePkFilter() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        singletonList("f0"),
                        singletonList("f1"),
                        r -> r.getInt(0) < 3,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(sequence(row(1, 11, 111), -1L)).iterator(), false);
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(sequence(row(1, 22, 222), -1L)).iterator(), false);
        assertThat(table.get(row(11))).hasSize(0);
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(sequence(row(2, 22, 222), -1L)).iterator(), false);
        result = table.get(row(22));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 22, 222);
        assertRow(result.get(1), 2, 22, 222);

        table.refresh(
                singletonList(sequence(row(RowKind.DELETE, 2, 22, 222), -1L)).iterator(), false);
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(sequence(row(3, 33, 333), -1L)).iterator(), false);
        assertThat(table.get(row(33))).hasSize(0);
    }

    @Test
    public void testNoPrimaryKeyTable() throws IOException, BulkLoader.WriteException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        Collections.emptyList(),
                        singletonList("f1"),
                        r -> true,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        // test bulk load 100_000 records
        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        Random rnd = new Random();
        Map<Integer, List<Integer>> joinKeyToFirst = new HashMap<>();
        for (int i = 1; i <= 100_000; i++) {
            int joinKey = rnd.nextInt(i);
            InternalRow row = row(i, joinKey, 111 * i);
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(row)));
            joinKeyToFirst.computeIfAbsent(joinKey, k -> new ArrayList<>()).add(i);
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        for (Map.Entry<Integer, List<Integer>> entry : joinKeyToFirst.entrySet()) {
            List<InternalRow> result = table.get(row(entry.getKey()));
            assertThat(result.stream().map(row -> row.getInt(0)))
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }

        // add new join key value
        table.refresh(singletonList(row(1, 22, 333)).iterator(), false);
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);
    }

    @Test
    public void testNoPrimaryKeyTableFilter() throws IOException {
        LookupTable table =
                LookupTable.create(
                        stateFactory,
                        rowType,
                        Collections.emptyList(),
                        singletonList("f1"),
                        r -> r.getInt(2) < 222,
                        ThreadLocalRandom.current().nextInt(2) * 10);

        table.refresh(singletonList(row(1, 11, 333)).iterator(), false);
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(0);

        table.refresh(singletonList(row(1, 11, 111)).iterator(), false);
        result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 11, 111)).iterator(), false);
        result = table.get(row(11));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 11, 111);
        assertRow(result.get(1), 1, 11, 111);
    }

    private static InternalRow row(Object... values) {
        return row(RowKind.INSERT, values);
    }

    private static InternalRow row(RowKind kind, Object... values) {
        GenericRow row = new GenericRow(kind, values.length);

        for (int i = 0; i < values.length; ++i) {
            row.setField(i, values[i]);
        }

        return row;
    }

    private static InternalRow sequence(InternalRow row, long sequenceNumber) {
        return new JoinedRow(row.getRowKind(), row, GenericRow.of(sequenceNumber));
    }

    private static void assertRow(InternalRow resultRow, int... expected) {
        int[] results = new int[expected.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = resultRow.getInt(i);
        }
        assertThat(results).containsExactly(expected);
    }
}
