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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.lookup.FullCacheLookupTable.TableBulkLoader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SortUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

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
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.types.DataTypes.INT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LookupTable}. */
public class LookupTableTest extends TableTestBase {

    @TempDir Path tempDir;

    private RowType rowType;

    private IOManager ioManager;

    private FullCacheLookupTable table;

    @BeforeEach
    public void before() throws IOException {
        this.rowType = RowType.of(new IntType(), new IntType(), new IntType());
        this.ioManager = new IOManagerImpl(tempDir.toString());
    }

    @AfterEach
    public void after() throws IOException {
        if (table != null) {
            table.close();
        }
    }

    private FileStoreTable createTable(List<String> primaryKeys, Options options) throws Exception {
        Identifier identifier = new Identifier("default", "t");
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        primaryKeys,
                        options.toMap(),
                        null);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    @Test
    public void testPkTable() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

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
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(row)));
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
        table.refresh(singletonList(row(1, 22, 222)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // test refresh to delete
        table.refresh(singletonList(row(RowKind.DELETE, 1, 11, 111)).iterator());
        assertThat(table.get(row(1))).hasSize(0);

        table.refresh(singletonList(row(RowKind.DELETE, 3, 33, 333)).iterator());
        assertThat(table.get(row(3))).hasSize(0);
    }

    @Test
    public void testPkTableWithSequenceField() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f1");
        FileStoreTable storeTable = createTable(singletonList("f0"), options);
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            InternalRow row = row(i, 11 * i, 111 * i);
            records.add(Pair.of(table.toKeyBytes(row), table.toValueBytes(row)));
        }
        records.sort((o1, o2) -> SortUtil.compareBinary(o1.getKey(), o2.getKey()));
        TableBulkLoader bulkLoader = table.createBulkLoader();
        for (Pair<byte[], byte[]> kv : records) {
            bulkLoader.write(kv.getKey(), kv.getValue());
        }
        bulkLoader.finish();

        // test refresh to update
        table.refresh(singletonList(row(1, 22, 222)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // refresh with old sequence
        table.refresh(singletonList((row(1, 11, 333))).iterator());
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        // test refresh delete data with old sequence
        table.refresh(singletonList(row(RowKind.DELETE, 1, 11, 111)).iterator());
        assertThat(table.get(row(1))).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);
    }

    @Test
    public void testPkTableWithSequenceFieldProjection() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f2");
        options.set(CoreOptions.BUCKET, 1);
        FileStoreTable storeTable = createTable(singletonList("f0"), options);
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // first write
        write(storeTable, GenericRow.of(1, 11, 111));
        table.refresh();
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11);

        // second write
        write(storeTable, GenericRow.of(1, 22, 222));
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22);

        // not update
        write(storeTable, GenericRow.of(1, 33, 111));
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22);
    }

    @Test
    public void testPkTablePkFilter() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        new PredicateBuilder(RowType.of(INT())).lessThan(0, 3),
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(RowKind.DELETE, 1, 11, 111)).iterator());
        assertThat(table.get(row(1))).hasSize(0);

        table.refresh(singletonList(row(3, 33, 333)).iterator());
        assertThat(table.get(row(3))).hasSize(0);
    }

    @Test
    public void testPkTableNonPkFilter() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        new PredicateBuilder(RowType.of(INT(), INT())).lessThan(1, 22),
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        result = table.get(row(1));
        assertThat(result).hasSize(0);
    }

    @Test
    public void testSecKeyTable() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        // test bulk load 100_000 records
        List<Pair<byte[], byte[]>> records = new ArrayList<>();
        Random rnd = new Random();
        Map<Integer, Set<Integer>> secKeyToPk = new HashMap<>();
        for (int i = 1; i <= 100_000; i++) {
            int secKey = rnd.nextInt(i);
            InternalRow row = row(i, secKey, 111 * i);
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

        // add new sec key to pk
        table.refresh(singletonList(row(1, 22, 222)).iterator());
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);
    }

    @Test
    public void testSecKeyTableWithSequenceField() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.SEQUENCE_FIELD, "f1");
        FileStoreTable storeTable = createTable(singletonList("f0"), options);
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

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

        // add new sec key to pk
        table.refresh(singletonList(row(1, 22, 222)).iterator());
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);
        assertThat(result.stream().map(InternalRow::getFieldCount)).allMatch(n -> n == 3);

        // refresh with old value
        table.refresh(singletonList(row(1, 11, 333)).iterator());
        result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(2))).doesNotContain(333);
    }

    @Test
    public void testSecKeyTablePkFilter() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        new PredicateBuilder(RowType.of(INT())).lessThan(0, 3),
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 22, 222)).iterator());
        assertThat(table.get(row(11))).hasSize(0);
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(2, 22, 222)).iterator());
        result = table.get(row(22));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 22, 222);
        assertRow(result.get(1), 2, 22, 222);

        table.refresh(singletonList(row(RowKind.DELETE, 2, 22, 222)).iterator());
        result = table.get(row(22));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222);

        table.refresh(singletonList(row(3, 33, 333)).iterator());
        assertThat(table.get(row(33))).hasSize(0);
    }

    @Test
    public void testNoPrimaryKeyTable() throws Exception {
        FileStoreTable storeTable = createTable(emptyList(), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

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
        table.refresh(singletonList(row(1, 22, 333)).iterator());
        List<InternalRow> result = table.get(row(22));
        assertThat(result.stream().map(row -> row.getInt(0))).contains(1);
    }

    @Test
    public void testNoPrimaryKeyTableFilter() throws Exception {
        FileStoreTable storeTable = createTable(emptyList(), new Options());
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        new PredicateBuilder(RowType.of(INT(), INT(), INT())).lessThan(2, 222),
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // test re-open
        table.close();
        table.open();

        table.refresh(singletonList(row(1, 11, 333)).iterator());
        List<InternalRow> result = table.get(row(11));
        assertThat(result).hasSize(0);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        result = table.get(row(11));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        table.refresh(singletonList(row(1, 11, 111)).iterator());
        result = table.get(row(11));
        assertThat(result).hasSize(2);
        assertRow(result.get(0), 1, 11, 111);
        assertRow(result.get(1), 1, 11, 111);
    }

    @Test
    public void testPkTableWithCacheRowFilter() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(1, 11, 111), GenericRow.of(2, 22, 222));

        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        assertThat(table).isInstanceOf(PrimaryKeyLookupTable.class);
        table.specifyCacheRowFilter(row -> row.getInt(0) < 2);
        table.open();

        List<InternalRow> res = table.get(GenericRow.of(1));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 1, 11, 111);

        res = table.get(GenericRow.of(2));
        assertThat(res).isEmpty();

        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(0, 0, 0), GenericRow.of(3, 33, 333));
        res = table.get(GenericRow.of(0));
        assertThat(res).isEmpty();

        table.refresh();
        res = table.get(GenericRow.of(0));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 0, 0, 0);

        res = table.get(GenericRow.of(3));
        assertThat(res).isEmpty();
    }

    @Test
    public void testNoPkTableWithCacheRowFilter() throws Exception {
        FileStoreTable storeTable = createTable(emptyList(), new Options());
        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(1, 11, 111), GenericRow.of(2, 22, 222));

        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        assertThat(table).isInstanceOf(NoPrimaryKeyLookupTable.class);
        table.specifyCacheRowFilter(row -> row.getInt(0) < 2);
        table.open();

        List<InternalRow> res = table.get(GenericRow.of(1));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 1, 11, 111);

        res = table.get(GenericRow.of(2));
        assertThat(res).isEmpty();

        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(0, 0, 0), GenericRow.of(3, 33, 333));
        res = table.get(GenericRow.of(0));
        assertThat(res).isEmpty();

        table.refresh();
        res = table.get(GenericRow.of(0));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 0, 0, 0);

        res = table.get(GenericRow.of(3));
        assertThat(res).isEmpty();
    }

    @Test
    public void testSecKeyTableWithCacheRowFilter() throws Exception {
        FileStoreTable storeTable = createTable(singletonList("f0"), new Options());
        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(1, 11, 111), GenericRow.of(2, 22, 222));

        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f1"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        assertThat(table).isInstanceOf(SecondaryIndexLookupTable.class);
        table.specifyCacheRowFilter(row -> row.getInt(1) < 22);
        table.open();

        List<InternalRow> res = table.get(GenericRow.of(11));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 1, 11, 111);

        res = table.get(GenericRow.of(22));
        assertThat(res).isEmpty();

        writeWithBucketAssigner(
                storeTable, row -> 0, GenericRow.of(0, 0, 0), GenericRow.of(3, 33, 333));
        res = table.get(GenericRow.of(0));
        assertThat(res).isEmpty();

        table.refresh();
        res = table.get(GenericRow.of(0));
        assertThat(res).hasSize(1);
        assertRow(res.get(0), 0, 0, 0);

        res = table.get(GenericRow.of(33));
        assertThat(res).isEmpty();
    }

    @Test
    public void testPartialLookupTable() throws Exception {
        FileStoreTable dimTable = createDimTable();
        PrimaryKeyPartialLookupTable table =
                PrimaryKeyPartialLookupTable.createLocalTable(
                        dimTable,
                        new int[] {0, 1, 2},
                        tempDir.toFile(),
                        ImmutableList.of("pk1", "pk2"),
                        null);
        table.open();

        List<InternalRow> result = table.get(row(1, -1));
        assertThat(result).hasSize(0);

        write(dimTable, ioManager, GenericRow.of(1, -1, 11), GenericRow.of(2, -2, 22));
        result = table.get(row(1, -1));
        assertThat(result).hasSize(0);

        table.refresh();
        result = table.get(row(1, -1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, -1, 11);
        result = table.get(row(2, -2));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 2, -2, 22);

        write(dimTable, ioManager, GenericRow.ofKind(RowKind.DELETE, 1, -1, 11));
        table.refresh();
        result = table.get(row(1, -1));
        assertThat(result).hasSize(0);
    }

    @Test
    public void testPartialLookupTableWithProjection() throws Exception {
        FileStoreTable dimTable = createDimTable();
        PrimaryKeyPartialLookupTable table =
                PrimaryKeyPartialLookupTable.createLocalTable(
                        dimTable,
                        new int[] {2, 1},
                        tempDir.toFile(),
                        ImmutableList.of("pk1", "pk2"),
                        null);
        table.open();

        // test re-open
        table.close();
        table.open();

        List<InternalRow> result = table.get(row(1, -1));
        assertThat(result).hasSize(0);

        write(dimTable, ioManager, GenericRow.of(1, -1, 11), GenericRow.of(2, -2, 22));
        result = table.get(row(1, -1));
        assertThat(result).hasSize(0);

        table.refresh();
        result = table.get(row(1, -1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 11, -1);
        result = table.get(row(2, -2));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 22, -2);
    }

    @Test
    public void testPartialLookupTableJoinKeyOrder() throws Exception {
        FileStoreTable dimTable = createDimTable();
        PrimaryKeyPartialLookupTable table =
                PrimaryKeyPartialLookupTable.createLocalTable(
                        dimTable,
                        new int[] {2, 1},
                        tempDir.toFile(),
                        ImmutableList.of("pk2", "pk1"),
                        null);
        table.open();

        // test re-open
        table.close();
        table.open();

        List<InternalRow> result = table.get(row(-1, 1));
        assertThat(result).hasSize(0);

        write(dimTable, ioManager, GenericRow.of(1, -1, 11), GenericRow.of(2, -2, 22));
        result = table.get(row(-1, 1));
        assertThat(result).hasSize(0);

        table.refresh();
        result = table.get(row(-1, 1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 11, -1);
        result = table.get(row(-2, 2));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 22, -2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPKLookupTableRefreshAsync(boolean refreshAsync) throws Exception {
        Options options = new Options();
        options.set(FlinkConnectorOptions.LOOKUP_REFRESH_ASYNC, refreshAsync);
        FileStoreTable storeTable = createTable(singletonList("f0"), options);
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);
        table.open();

        // Batch insert 100_000 records into table store
        BatchWriteBuilder writeBuilder = storeTable.newBatchWriteBuilder();
        Set<Integer> insertKeys = new HashSet<>();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            for (int i = 1; i <= 100_000; i++) {
                insertKeys.add(i);
                write.write(row(i, 11 * i, 111 * i), 0);
            }
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }

        // Refresh lookup table
        table.refresh();
        Set<Integer> batchKeys = new HashSet<>();
        long start = System.currentTimeMillis();
        while (batchKeys.size() < 100_000) {
            Thread.sleep(10);
            for (int i = 1; i <= 100_000; i++) {
                List<InternalRow> result = table.get(row(i));
                if (!result.isEmpty()) {
                    assertThat(result).hasSize(1);
                    assertRow(result.get(0), i, 11 * i, 111 * i);
                    batchKeys.add(i);
                }
            }
            if (System.currentTimeMillis() - start > 30_000) {
                throw new TimeoutException();
            }
        }
        assertThat(batchKeys).isEqualTo(insertKeys);

        // Add 10 snapshots and refresh lookup table
        for (int k = 0; k < 10; k++) {
            try (BatchTableWrite write = writeBuilder.newWrite()) {
                for (int i = 1; i <= 100; i++) {
                    write.write(row(i, 11 * i, 111 * i), 0);
                }
                try (BatchTableCommit commit = writeBuilder.newCommit()) {
                    commit.commit(write.prepareCommit());
                }
            }
        }
        table.refresh();

        table.close();
    }

    @Test
    public void testFullCacheLookupTableWithForceLookup() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.set(
                FlinkConnectorOptions.LOOKUP_CACHE_MODE,
                FlinkConnectorOptions.LookupCacheMode.FULL);
        options.set(CoreOptions.WRITE_ONLY, true);
        options.set(CoreOptions.FORCE_LOOKUP, true);
        options.set(CoreOptions.BUCKET, 1);
        FileStoreTable storeTable = createTable(singletonList("f0"), options);
        FileStoreTable compactTable =
                storeTable.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
        FullCacheLookupTable.Context context =
                new FullCacheLookupTable.Context(
                        storeTable,
                        new int[] {0, 1, 2},
                        null,
                        null,
                        tempDir.toFile(),
                        singletonList("f0"),
                        null);
        table = FullCacheLookupTable.create(context, ThreadLocalRandom.current().nextInt(2) * 10);

        // initialize
        write(storeTable, ioManager, GenericRow.of(1, 11, 111));
        compact(compactTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        table.open();

        List<InternalRow> result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111);

        // first write
        write(storeTable, GenericRow.of(1, null, 222));
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 111); // old value because there is no compact

        // only L0 occur compact
        compact(compactTable, BinaryRow.EMPTY_ROW, 0, ioManager, false);
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 222); // get new value after compact

        // second write
        write(storeTable, GenericRow.of(1, 22, null));
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 11, 222); // old value

        // full compact
        compact(compactTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, 22, 222); // new value
    }

    @Test
    public void testPartialLookupTableWithForceLookup() throws Exception {
        Options options = new Options();
        options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.PARTIAL_UPDATE);
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.NONE);
        options.set(CoreOptions.FORCE_LOOKUP, true);
        options.set(CoreOptions.BUCKET, 1);
        FileStoreTable dimTable = createTable(singletonList("f0"), options);

        PrimaryKeyPartialLookupTable table =
                PrimaryKeyPartialLookupTable.createLocalTable(
                        dimTable,
                        new int[] {0, 1, 2},
                        tempDir.toFile(),
                        ImmutableList.of("f0"),
                        null);
        table.open();

        List<InternalRow> result = table.get(row(1, -1));
        assertThat(result).hasSize(0);

        write(dimTable, ioManager, GenericRow.of(1, -1, 11), GenericRow.of(2, -2, 22));
        result = table.get(row(1));
        assertThat(result).hasSize(0);

        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, -1, 11);
        result = table.get(row(2));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 2, -2, 22);

        write(dimTable, ioManager, GenericRow.of(1, null, 111));
        table.refresh();
        result = table.get(row(1));
        assertThat(result).hasSize(1);
        assertRow(result.get(0), 1, -1, 111);
    }

    private FileStoreTable createDimTable() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        org.apache.paimon.fs.Path tablePath =
                new org.apache.paimon.fs.Path(
                        String.format("%s/%s.db/%s", warehouse, database, "T"));
        Schema schema =
                Schema.newBuilder()
                        .column("pk1", INT())
                        .column("pk2", INT())
                        .column("col2", INT())
                        .primaryKey("pk1", "pk2")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.BUCKET_KEY.key(), "pk2")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
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

    private static void assertRow(InternalRow resultRow, int... expected) {
        int[] results = new int[expected.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = resultRow.getInt(i);
        }
        assertThat(results).containsExactly(expected);
        assertThat(resultRow.getFieldCount()).isEqualTo(expected.length);
    }

    private void writeAndCommit(FileStoreTable table, InternalRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite();
                BatchTableCommit commiter = builder.newCommit()) {
            for (InternalRow row : rows) {
                writer.write(row, 0);
            }
            commiter.commit(writer.prepareCommit());
        }
    }
}
