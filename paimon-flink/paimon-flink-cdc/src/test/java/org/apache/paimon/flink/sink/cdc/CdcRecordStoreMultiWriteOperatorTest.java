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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableTypeInfo;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordStoreMultiWriteOperator}. */
public class CdcRecordStoreMultiWriteOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private String commitUser;
    private Path warehouse;
    private String databaseName;
    private Identifier firstTable;
    private Catalog catalog;
    private Identifier secondTable;
    private Catalog.Loader catalogLoader;
    private Schema firstTableSchema;

    @BeforeEach
    public void before() throws Exception {
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();

        databaseName = "test_db";
        firstTable = Identifier.create(databaseName, "test_table1");
        secondTable = Identifier.create(databaseName, "test_table2");

        catalogLoader = createCatalogLoader();
        catalog = catalogLoader.load();
        catalog.createDatabase(databaseName, true);
        Options conf = new Options();
        conf.set(CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));
        conf.set(CoreOptions.BUCKET, 1);

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"pt", "k", "v"});
        RowType rowType2 =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.FLOAT(),
                            DataTypes.VARCHAR(5),
                            DataTypes.VARBINARY(5)
                        },
                        new String[] {"k", "v1", "v2", "v3", "v4"});

        firstTableSchema =
                new Schema(
                        rowType1.getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        conf.toMap(),
                        "");
        createTestTables(
                catalog,
                Tuple2.of(firstTable, firstTableSchema),
                Tuple2.of(
                        secondTable,
                        new Schema(
                                rowType2.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                conf.toMap(),
                                "")));
    }

    private void createTestTables(Catalog catalog, Tuple2<Identifier, Schema>... tableSpecs)
            throws Exception {

        for (Tuple2<Identifier, Schema> spec : tableSpecs) {
            catalog.createTable(spec.f0, spec.f1, false);
        }
    }

    @AfterEach
    public void after() throws Exception {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();

        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    @Timeout(30)
    public void testAsyncTableCreate() throws Exception {
        // the async table will have same row type, partitions, and pks as firstTable
        Identifier tableId = Identifier.create(databaseName, "async_new_table");

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);

        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records should be processed after table is created
        Map<String, String> data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "1");
        data.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.poll(1);

        assertThat(actual).isNull();

        catalog.createTable(tableId, firstTableSchema, true);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // after table is created, record should be processed immediately
        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "3");
        data.put("v", "30");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testInitializeState() throws Exception {
        // the async table will have same row type, partitions, and pks as firstTable
        Identifier tableId = Identifier.create(databaseName, "async_new_table");
        long timestamp = 1;

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);

        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records should be processed after table is created
        Map<String, String> data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "1");
        data.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.poll(1);

        assertThat(actual).isNull();

        CdcRecordStoreMultiWriteOperator operator =
                (CdcRecordStoreMultiWriteOperator) harness.getOperator();
        assertThat(operator.tables().size()).isEqualTo(0);
        assertThat(operator.writes().size()).isEqualTo(0);

        catalog.createTable(tableId, firstTableSchema, true);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);
        assertThat(operator.tables().size()).isEqualTo(1);
        assertThat(operator.writes().size()).isEqualTo(1);

        // after table is created, record should be processed immediately
        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "3");
        data.put("v", "30");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // trigger a snapshot and re-create test harness using a different commit user
        OperatorSubtaskState snapshot = harness.snapshot(0, timestamp++);
        String prevCommitUser = commitUser;
        commitUser = UUID.randomUUID().toString();
        harness.close();

        harness = createTestHarness(catalogLoader);
        harness.initializeState(snapshot);
        operator = (CdcRecordStoreMultiWriteOperator) harness.getOperator();

        assertThat(operator.commitUser()).isEqualTo(prevCommitUser);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testSingleTableAddColumn() throws Exception {

        Identifier tableId = firstTable;
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);

        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "1");
        data.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "2");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new data should be processed after schema is updated

        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "3");
        data.put("v", "30");
        data.put("v2", "300");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.addColumn("v2", DataTypes.INT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    private Catalog.Loader createCatalogLoader() {
        Options catalogOptions = createCatalogOptions(warehouse);
        return () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
    }

    private Options createCatalogOptions(Path warehouse) {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, warehouse.toString());
        conf.set(CatalogOptions.URI, "");

        return conf;
    }

    @Test
    @Timeout(30)
    public void testSingleTableUpdateColumnType() throws Exception {
        Identifier tableId = secondTable;
        FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema can be processed immediately

        Map<String, String> data = new HashMap<>();
        data.put("k", "1");
        data.put("v1", "10");
        data.put("v2", "0.625");
        data.put("v3", "one");
        data.put("v4", "b_one");
        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new data should be processed after schema is updated

        // int -> bigint

        data = new HashMap<>();
        data.put("k", "2");
        data.put("v1", "12345678987654321");
        data.put("v2", "0.25");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v1", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // float -> double

        data = new HashMap<>();
        data.put("k", "3");
        data.put("v1", "100");
        data.put("v2", "1.0000000000009095");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v2", DataTypes.DOUBLE()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varchar(5) -> varchar(10)

        data = new HashMap<>();
        data.put("k", "4");
        data.put("v1", "40");
        data.put("v3", "long four");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v3", DataTypes.VARCHAR(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varbinary(5) -> varbinary(10)

        data = new HashMap<>();
        data.put("k", "5");
        data.put("v1", "50");
        data.put("v4", "long five~");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName, tableId.getObjectName(), new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v4", DataTypes.VARBINARY(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testMultiTableUpdateColumnType() throws Exception {
        FileStoreTable table1 = (FileStoreTable) catalog.getTable(firstTable);
        FileStoreTable table2 = (FileStoreTable) catalog.getTable(secondTable);

        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // check that records with compatible schema from different tables
        //     can be processed immediately

        Map<String, String> data;

        // first table record
        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "1");
        data.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        firstTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        CdcMultiplexRecord actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // second table record
        data = new HashMap<>();
        data.put("k", "1");
        data.put("v1", "10");
        data.put("v2", "0.625");
        data.put("v3", "one");
        data.put("v4", "b_one");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // check that records with new data should be processed after schema is updated

        // int -> bigint
        SchemaManager schemaManager;
        // first table
        data = new HashMap<>();
        data.put("pt", "1");
        data.put("k", "123456789876543211");
        data.put("v", "varchar");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        firstTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table1.fileIO(), table1.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("k", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // second table
        data = new HashMap<>();
        data.put("k", "2");
        data.put("v1", "12345678987654321");
        data.put("v2", "0.25");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v1", DataTypes.BIGINT()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // below are schema changes only from the second table
        // float -> double

        data = new HashMap<>();
        data.put("k", "3");
        data.put("v1", "100");
        data.put("v2", "1.0000000000009095");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v2", DataTypes.DOUBLE()));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varchar(5) -> varchar(10)

        data = new HashMap<>();
        data.put("k", "4");
        data.put("v1", "40");
        data.put("v3", "long four");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager = new SchemaManager(table2.fileIO(), table2.location());
        schemaManager.commitChanges(SchemaChange.updateColumnType("v3", DataTypes.VARCHAR(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        // varbinary(5) -> varbinary(10)

        data = new HashMap<>();
        data.put("k", "5");
        data.put("v1", "50");
        data.put("v4", "long five~");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);
        actual = runner.poll(1);
        assertThat(actual).isNull();

        schemaManager.commitChanges(SchemaChange.updateColumnType("v4", DataTypes.VARBINARY(10)));
        actual = runner.take();
        assertThat(actual).isEqualTo(expected);

        runner.stop();
        t.join();
        harness.close();
    }

    @Test
    @Timeout(30)
    public void testUsingTheSameCompactExecutor() throws Exception {
        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                createTestHarness(catalogLoader);
        harness.open();

        Runner runner = new Runner(harness);
        Thread t = new Thread(runner);
        t.start();

        // write records to two tables thus two FileStoreWrite will be created
        Map<String, String> data;

        // first table record
        data = new HashMap<>();
        data.put("pt", "0");
        data.put("k", "1");
        data.put("v", "10");

        CdcMultiplexRecord expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        firstTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);

        // second table record
        data = new HashMap<>();
        data.put("k", "1");
        data.put("v1", "10");
        data.put("v2", "0.625");
        data.put("v3", "one");
        data.put("v4", "b_one");
        expected =
                CdcMultiplexRecord.fromCdcRecord(
                        databaseName,
                        secondTable.getObjectName(),
                        new CdcRecord(RowKind.INSERT, data));
        runner.offer(expected);

        // get and check compactExecutor from two FileStoreWrite
        CdcRecordStoreMultiWriteOperator operator =
                (CdcRecordStoreMultiWriteOperator) harness.getOperator();
        CommonTestUtils.waitUtil(
                () -> operator.writes().size() == 2, Duration.ofSeconds(5), Duration.ofMillis(100));

        List<StoreSinkWrite> storeSinkWrites = new ArrayList<>(operator.writes().values());
        List<ExecutorService> compactExecutors = new ArrayList<>();
        for (StoreSinkWrite storeSinkWrite : storeSinkWrites) {
            StoreSinkWriteImpl storeSinkWriteImpl = (StoreSinkWriteImpl) storeSinkWrite;
            compactExecutors.add(
                    ((AbstractFileStoreWrite<?>) storeSinkWriteImpl.getWrite().getWrite())
                            .getCompactExecutor());
        }
        assertThat(compactExecutors.get(0) == compactExecutors.get(1)).isTrue();

        // check that compactExecutor should be shutdown by operator
        ExecutorService compactExecutor = compactExecutors.get(0);
        for (StoreSinkWrite storeSinkWrite : storeSinkWrites) {
            storeSinkWrite.close();
            assertThat(compactExecutor.isShutdown()).isFalse();
        }

        operator.close();
        assertThat(compactExecutor.isShutdown()).isTrue();

        runner.stop();
        t.join();
        harness.close();
    }

    private OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable>
            createTestHarness(Catalog.Loader catalogLoader) throws Exception {
        CdcRecordStoreMultiWriteOperator operator =
                new CdcRecordStoreMultiWriteOperator(
                        catalogLoader,
                        (t, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                                new StoreSinkWriteImpl(
                                        t,
                                        commitUser,
                                        state,
                                        ioManager,
                                        false,
                                        false,
                                        true,
                                        memoryPoolFactory,
                                        metricGroup),
                        commitUser,
                        Options.fromMap(new HashMap<>()));
        TypeSerializer<CdcMultiplexRecord> inputSerializer = new JavaSerializer<>();
        TypeSerializer<MultiTableCommittable> outputSerializer =
                new MultiTableCommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable> harness =
                new OneInputStreamOperatorTestHarness<>(operator, inputSerializer);
        harness.setup(outputSerializer);
        return harness;
    }

    private static class Runner implements Runnable {

        private final OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable>
                harness;
        private final BlockingQueue<CdcMultiplexRecord> toProcess = new LinkedBlockingQueue<>();
        private final BlockingQueue<CdcMultiplexRecord> processed = new LinkedBlockingQueue<>();
        private final AtomicBoolean running = new AtomicBoolean(true);

        private Runner(
                OneInputStreamOperatorTestHarness<CdcMultiplexRecord, MultiTableCommittable>
                        harness) {
            this.harness = harness;
        }

        private void offer(CdcMultiplexRecord record) {
            toProcess.offer(record);
        }

        private CdcMultiplexRecord take() throws Exception {
            return processed.take();
        }

        private CdcMultiplexRecord poll(long seconds) throws Exception {
            return processed.poll(seconds, TimeUnit.SECONDS);
        }

        private void stop() {
            running.set(false);
        }

        @Override
        public void run() {
            long timestamp = 0;
            try {
                while (running.get()) {
                    if (toProcess.isEmpty()) {
                        Thread.sleep(10);
                        continue;
                    }

                    CdcMultiplexRecord record = toProcess.poll();
                    harness.processElement(record, ++timestamp);
                    processed.offer(record);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
