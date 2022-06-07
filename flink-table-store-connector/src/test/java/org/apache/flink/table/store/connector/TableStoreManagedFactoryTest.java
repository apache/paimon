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

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.RowDataPartitionComputer;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.predicate.PredicateConverter;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.file.utils.PartitionedManifestMeta;
import org.apache.flink.table.store.log.LogOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_MANUAL_TRIGGERED;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_PARTITION_SPEC;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_RESCALE_BUCKET;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.COMPACTION_SCANNED_MANIFEST;
import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.ROOT_PATH;
import static org.apache.flink.table.store.connector.TableStoreTestBase.createResolvedTable;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.flink.table.store.file.FileStoreOptions.PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.file.FileStoreOptions.path;
import static org.apache.flink.table.store.file.FileStoreOptions.relativeTablePath;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.DEFAULT_ROW_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.GeneratorMode.SINGLE_PARTITIONED;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.KEY_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.NON_PARTITIONED_ROW_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.SINGLE_PARTITIONED_ROW_TYPE;
import static org.apache.flink.table.store.file.TestKeyValueGenerator.getPrimaryKeys;
import static org.apache.flink.table.store.file.mergetree.MergeTreeOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER;
import static org.apache.flink.table.store.file.mergetree.MergeTreeOptions.NUM_SORTED_RUNS_STOP_TRIGGER;
import static org.apache.flink.table.store.file.utils.FileStorePathFactory.getPartitionComputer;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.CONSISTENCY;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test cases for {@link TableStoreManagedFactory}. */
public class TableStoreManagedFactoryTest {

    private static final String CATALOG = "catalog";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final ObjectIdentifier TABLE_IDENTIFIER =
            ObjectIdentifier.of(CATALOG, DATABASE, TABLE);
    private static final int NUM_OF_BUCKETS = 2;

    private final TableStoreManagedFactory tableStoreManagedFactory =
            new TableStoreManagedFactory();

    @TempDir private static java.nio.file.Path sharedTempDir;
    private DynamicTableFactory.Context context;

    @ParameterizedTest
    @MethodSource("provideOptionsToEnrich")
    public void testEnrichOptions(
            Map<String, String> sessionOptions,
            Map<String, String> tableOptions,
            Map<String, String> expectedEnrichedOptions) {
        context = createNonEnrichedContext(sessionOptions, tableOptions);
        Map<String, String> actualEnrichedOptions = tableStoreManagedFactory.enrichOptions(context);
        assertThat(actualEnrichedOptions)
                .containsExactlyInAnyOrderEntriesOf(expectedEnrichedOptions);
    }

    @Test
    public void testErrorEnrichOptions() {
        Map<String, String> sessionMap = new HashMap<>();
        sessionMap.put("table-store.root-path", "my_path");
        sessionMap.put("table-store.path", "another_path");
        context = createNonEnrichedContext(sessionMap, emptyMap());
        assertThatThrownBy(() -> tableStoreManagedFactory.enrichOptions(context))
                .hasMessage(
                        "Managed table can not contain table path. You need to remove path in table options or session config.");

        context = createNonEnrichedContext(emptyMap(), emptyMap());
        assertThatThrownBy(() -> tableStoreManagedFactory.enrichOptions(context))
                .hasMessage(
                        "Please specify a root path by setting session level configuration as `SET 'table-store.root-path' = '...'`.");
    }

    @Test
    public void testEnrichKafkaTopic() {
        Map<String, String> sessionMap = new HashMap<>();
        sessionMap.put("table-store.root-path", "my_path");
        sessionMap.put("table-store.log.system", "kafka");
        sessionMap.put("table-store.log.topic", "my_topic");
        context = createNonEnrichedContext(sessionMap, emptyMap());
        assertThatThrownBy(() -> tableStoreManagedFactory.enrichOptions(context))
                .hasMessage(
                        "Managed table can not contain custom topic. You need to remove topic in table options or session config.");

        sessionMap.remove("table-store.log.topic");
        context = createNonEnrichedContext(sessionMap, emptyMap());
        Map<String, String> enriched = tableStoreManagedFactory.enrichOptions(context);

        Map<String, String> expected = new HashMap<>();
        expected.put("path", "my_path/catalog.catalog/database.db/table");
        expected.put("log.system", "kafka");
        expected.put("log.topic", "catalog.database.table");
        assertThat(enriched).containsExactlyEntriesOf(expected);
    }

    @ParameterizedTest
    @MethodSource("provideOptionsToCreate")
    public void testOnCreateTable(Map<String, String> enrichedOptions, boolean ignoreIfExists) {
        context = createEnrichedContext(enrichedOptions);
        Path expectedPath = Paths.get(path(enrichedOptions).getPath());
        boolean exist = expectedPath.toFile().exists();
        if (ignoreIfExists || !exist) {
            tableStoreManagedFactory.onCreateTable(context, ignoreIfExists);
            assertThat(expectedPath).exists();
        } else {
            assertThatThrownBy(() -> tableStoreManagedFactory.onCreateTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to create file store path. "
                                            + "Reason: directory %s exists for table %s. "
                                            + "Suggestion: please try `DESCRIBE TABLE %s` to "
                                            + "first check whether table exists in current catalog. "
                                            + "If table exists in catalog, and data files under current path "
                                            + "are valid, please use `CREATE TABLE IF NOT EXISTS` ddl instead. "
                                            + "Otherwise, please choose another table name "
                                            + "or manually delete the current path and try again.",
                                    expectedPath,
                                    TABLE_IDENTIFIER.asSerializableString(),
                                    TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    @ParameterizedTest
    @MethodSource("provideOptionsToDrop")
    public void testOnDropTable(Map<String, String> enrichedOptions, boolean ignoreIfNotExists) {
        context = createEnrichedContext(enrichedOptions);
        Path expectedPath = Paths.get(path(enrichedOptions).getPath());
        boolean exist = expectedPath.toFile().exists();
        if (exist || ignoreIfNotExists) {
            tableStoreManagedFactory.onDropTable(context, ignoreIfNotExists);
            assertThat(expectedPath).doesNotExist();
        } else {
            assertThatThrownBy(() -> tableStoreManagedFactory.onDropTable(context, false))
                    .isInstanceOf(TableException.class)
                    .hasMessageContaining(
                            String.format(
                                    "Failed to delete file store path. "
                                            + "Reason: directory %s doesn't exist for table %s. "
                                            + "Suggestion: please try `DROP TABLE IF EXISTS` ddl instead.",
                                    expectedPath, TABLE_IDENTIFIER.asSerializableString()));
        }
    }

    @Test
    public void testFilterLogStoreOptions() {
        // mix invalid key and leave value to empty to emphasize the deferred validation
        Map<String, String> expectedLogOptions =
                of(
                        LogOptions.SCAN.key(),
                        "",
                        LogOptions.RETENTION.key(),
                        "",
                        "dummy.key",
                        "",
                        LogOptions.CHANGELOG_MODE.key(),
                        "");
        Map<String, String> enrichedOptions =
                addPrefix(expectedLogOptions, LOG_PREFIX, (key) -> true);
        enrichedOptions.put("foo", "bar");

        assertThat(TableStoreManagedFactory.filterLogStoreOptions(enrichedOptions))
                .containsExactlyInAnyOrderEntriesOf(expectedLogOptions);
    }

    @ParameterizedTest
    @MethodSource("provideResolvedTable")
    public void testCreateAndCheckTableStore(
            RowType rowType,
            List<String> partitions,
            List<String> primaryKeys,
            TableStoreTestBase.ExpectedResult expectedResult) {
        ResolvedCatalogTable catalogTable =
                createResolvedTable(
                        singletonMap(
                                PATH.key(),
                                sharedTempDir.toAbsolutePath() + "/" + UUID.randomUUID()),
                        rowType,
                        partitions,
                        primaryKeys);
        context = createEnrichedContext(TABLE_IDENTIFIER, catalogTable);
        if (expectedResult.success) {
            tableStoreManagedFactory.onCreateTable(context, false);
            TableStore tableStore = tableStoreManagedFactory.buildTableStore(context);
            assertThat(tableStore.partitioned()).isEqualTo(catalogTable.isPartitioned());
            assertThat(tableStore.valueCountMode())
                    .isEqualTo(catalogTable.getResolvedSchema().getPrimaryKeyIndexes().length == 0);

            // check primary key doesn't contain partition
            if (tableStore.partitioned() && !tableStore.valueCountMode()) {
                assertThat(
                                tableStore.trimmedPrimaryKeys().stream()
                                        .noneMatch(pk -> tableStore.partitionKeys().contains(pk)))
                        .isTrue();
            }
        } else {
            assertThatThrownBy(() -> tableStoreManagedFactory.onCreateTable(context, false))
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);
        }
    }

    @Test
    public void testOnCompactTableForNoSnapshot() {
        RowType partType = RowType.of();
        MockTableStoreManagedFactory mockTableStoreManagedFactory =
                new MockTableStoreManagedFactory(partType, NON_PARTITIONED_ROW_TYPE);
        prepare(
                TABLE + "_" + UUID.randomUUID(),
                partType,
                NON_PARTITIONED_ROW_TYPE,
                NON_PARTITIONED,
                true);
        assertThatThrownBy(
                        () ->
                                mockTableStoreManagedFactory.onCompactTable(
                                        context, new CatalogPartitionSpec(emptyMap())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The specified table to rescale does not exist any snapshot");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOnCompactTableForNonPartitioned(boolean rescaleBucket) throws Exception {
        RowType partType = RowType.of();
        runTest(
                new MockTableStoreManagedFactory(partType, NON_PARTITIONED_ROW_TYPE),
                TABLE + "_" + UUID.randomUUID(),
                partType,
                NON_PARTITIONED_ROW_TYPE,
                NON_PARTITIONED,
                rescaleBucket);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOnCompactTableForSinglePartitioned(boolean rescaleBucket) throws Exception {
        runTest(
                new MockTableStoreManagedFactory(
                        SINGLE_PARTITIONED_PART_TYPE, SINGLE_PARTITIONED_ROW_TYPE),
                TABLE + "_" + UUID.randomUUID(),
                SINGLE_PARTITIONED_PART_TYPE,
                SINGLE_PARTITIONED_ROW_TYPE,
                SINGLE_PARTITIONED,
                rescaleBucket);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testOnCompactTableForMultiPartitioned(boolean rescaleBucket) throws Exception {
        runTest(
                new MockTableStoreManagedFactory(),
                TABLE + "_" + UUID.randomUUID(),
                DEFAULT_PART_TYPE,
                DEFAULT_ROW_TYPE,
                MULTI_PARTITIONED,
                rescaleBucket);
    }

    // ~ Tools ------------------------------------------------------------------

    private void runTest(
            MockTableStoreManagedFactory mockFactory,
            String tableName,
            RowType partitionType,
            RowType rowType,
            TestKeyValueGenerator.GeneratorMode mode,
            boolean rescaleBucket)
            throws Exception {
        String path = prepare(tableName, partitionType, rowType, mode, rescaleBucket);
        TestFileStore fileStore =
                TestFileStore.create(
                        "avro",
                        path,
                        NUM_OF_BUCKETS,
                        partitionType,
                        KEY_TYPE,
                        rowType,
                        new DeduplicateMergeFunction());

        Random random = new Random();
        TestKeyValueGenerator generator = new TestKeyValueGenerator(mode);
        int commitCount =
                MergeTreeOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.defaultValue()
                        + random.nextInt(5);
        List<KeyValue> committedData = new ArrayList<>();
        Snapshot snapshot = null;
        for (int i = 0; i < commitCount; i++) {
            List<KeyValue> data = generateData(generator, random.nextInt(1000));
            snapshot = commitData(fileStore, generator, data);
            committedData.addAll(data);
        }
        List<BinaryRowData> partitions =
                committedData.stream()
                        .map(generator::getPartition)
                        .distinct()
                        .collect(Collectors.toList());
        Map<String, String> partSpec;
        if (mode != NON_PARTITIONED) {
            RowDataPartitionComputer partitionComputer =
                    getPartitionComputer(partitionType, PARTITION_DEFAULT_NAME.defaultValue());

            List<Map<String, String>> partKvs =
                    partitions.stream()
                            .map(partitionComputer::generatePartValues)
                            .collect(Collectors.toList());
            partSpec = pickPartition(partKvs);
        } else {
            partSpec = emptyMap();
        }
        assertManifestTobeCompacted(
                mockFactory,
                rescaleBucket,
                fileStore,
                snapshot.id(),
                new CatalogPartitionSpec(partSpec));
    }

    private String prepare(
            String tableName,
            RowType partitionType,
            RowType rowType,
            TestKeyValueGenerator.GeneratorMode mode,
            boolean rescaleBucket) {
        int compactionTrigger = 20;
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of(CATALOG, DATABASE, tableName);
        String path =
                Paths.get(
                                sharedTempDir.toAbsolutePath().toString(),
                                relativeTablePath(tableIdentifier))
                        .toString();
        Map<String, String> options =
                of(
                        BUCKET.key(),
                        String.valueOf(NUM_OF_BUCKETS),
                        PATH.key(),
                        path,
                        NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(),
                        String.valueOf(compactionTrigger),
                        NUM_SORTED_RUNS_STOP_TRIGGER.key(),
                        String.valueOf(compactionTrigger));
        if (rescaleBucket) {
            options.put(COMPACTION_RESCALE_BUCKET.key(), String.valueOf(true));
        }
        ResolvedCatalogTable catalogTable =
                createResolvedTable(
                        options, rowType, partitionType.getFieldNames(), getPrimaryKeys(mode));
        context = createEnrichedContext(tableIdentifier, catalogTable);
        tableStoreManagedFactory.onCreateTable(context, false);
        return path;
    }

    private List<KeyValue> generateData(TestKeyValueGenerator generator, int numRecords) {
        List<KeyValue> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            data.add(generator.next());
        }
        return data;
    }

    private Snapshot commitData(
            TestFileStore fileStore, TestKeyValueGenerator generator, List<KeyValue> data)
            throws Exception {
        List<Snapshot> snapshots =
                fileStore.commitData(data, generator::getPartition, this::getBucket);
        return snapshots.get(snapshots.size() - 1);
    }

    private Integer getBucket(KeyValue kv) {
        return (kv.key().hashCode() % NUM_OF_BUCKETS + NUM_OF_BUCKETS) % NUM_OF_BUCKETS;
    }

    private void assertManifestTobeCompacted(
            MockTableStoreManagedFactory mockFactory,
            boolean rescaleBucket,
            FileStore fileStore,
            long snapshotId,
            CatalogPartitionSpec partitionSpec)
            throws IOException {
        Map<String, String> actual = mockFactory.onCompactTable(context, partitionSpec);
        if (rescaleBucket) {
            org.apache.flink.table.store.file.predicate.Predicate partFilter;
            if (partitionSpec.getPartitionSpec().isEmpty()) {
                partFilter = null;
            } else {
                partFilter =
                        PredicateConverter.CONVERTER.fromMap(
                                partitionSpec.getPartitionSpec(), fileStore.partitionType());
            }
            FileStoreScan.Plan expectedPlan =
                    fileStore
                            .newScan()
                            .withSnapshot(snapshotId)
                            .withPartitionFilter(partFilter)
                            .plan();
            assertThat(actual)
                    .containsEntry(
                            COMPACTION_SCANNED_MANIFEST.key(),
                            Base64.getEncoder()
                                    .encodeToString(
                                            InstantiationUtil.serializeObject(
                                                    new PartitionedManifestMeta(
                                                            expectedPlan.snapshotId(),
                                                            expectedPlan.groupByPartFiles()))));
            assertThat(actual).containsEntry(COMPACTION_RESCALE_BUCKET.key(), String.valueOf(true));
        } else {
            assertThat(actual)
                    .containsEntry(COMPACTION_MANUAL_TRIGGERED.key(), String.valueOf(true));
            assertThat(actual)
                    .containsEntry(
                            COMPACTION_PARTITION_SPEC.key(),
                            JsonSerdeUtil.toJson(partitionSpec.getPartitionSpec()));
        }
    }

    private static Map<String, String> pickPartition(List<Map<String, String>> partitions) {
        // pick a resolved partition spec
        Random random = new Random();
        Map<String, String> partition =
                new HashMap<>(partitions.get(random.nextInt(partitions.size())));
        // remove some partition keys to test partition resolution
        List<String> keys = new ArrayList<>(partition.keySet());
        int count = random.nextInt(keys.size());
        for (int i = 0; i < count; i++) {
            partition.remove(pickKey(keys));
        }
        return partition;
    }

    private static String pickKey(List<String> keys) {
        int idx = new Random().nextInt(keys.size());
        String key = keys.get(idx);
        keys.set(idx, keys.get(keys.size() - 1));
        keys.remove(keys.size() - 1);
        return key;
    }

    private static ResolvedCatalogTable getDummyTable(Map<String, String> tableOptions) {
        return new ResolvedCatalogTable(
                CatalogTable.of(Schema.derived(), "a comment", emptyList(), tableOptions),
                ResolvedSchema.of(emptyList()));
    }

    private static DynamicTableFactory.Context createNonEnrichedContext(
            Map<String, String> sessionOptions, Map<String, String> tableOptions) {
        return new FactoryUtil.DefaultDynamicTableContext(
                TABLE_IDENTIFIER,
                getDummyTable(tableOptions),
                emptyMap(),
                Configuration.fromMap(sessionOptions),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static DynamicTableFactory.Context createEnrichedContext(Map<String, String> options) {
        return createEnrichedContext(TABLE_IDENTIFIER, getDummyTable(options));
    }

    private static DynamicTableFactory.Context createEnrichedContext(
            ObjectIdentifier tableIdentifier, ResolvedCatalogTable catalogTable) {
        return new FactoryUtil.DefaultDynamicTableContext(
                tableIdentifier,
                catalogTable,
                emptyMap(),
                Configuration.fromMap(emptyMap()),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private static Stream<Arguments> provideOptionsToEnrich() {
        Map<String, String> enrichedOptions =
                of(
                        BUCKET.key(),
                        BUCKET.defaultValue().toString(),
                        ROOT_PATH.key(),
                        sharedTempDir.toString(),
                        LOG_PREFIX + BOOTSTRAP_SERVERS.key(),
                        "localhost:9092",
                        LOG_PREFIX + CONSISTENCY.key(),
                        CONSISTENCY.defaultValue().name());

        // set configuration under session level
        Arguments arg1 =
                Arguments.of(
                        addPrefix(enrichedOptions, TABLE_STORE_PREFIX, (key) -> true),
                        emptyMap(),
                        generateTablePath(enrichedOptions));

        // set configuration under table level
        Arguments arg2 =
                Arguments.of(emptyMap(), enrichedOptions, generateTablePath(enrichedOptions));

        // set both session and table level configuration to test options combination
        Map<String, String> tableOptions = new HashMap<>(enrichedOptions);
        tableOptions.remove(ROOT_PATH.key());
        tableOptions.remove(CONSISTENCY.key());
        Arguments arg3 =
                Arguments.of(
                        addPrefix(
                                enrichedOptions,
                                TABLE_STORE_PREFIX,
                                (key) -> !tableOptions.containsKey(key)),
                        tableOptions,
                        generateTablePath(enrichedOptions));

        // set same key with different value to test table configuration take precedence
        Map<String, String> sessionOptions = new HashMap<>();
        sessionOptions.put(
                TABLE_STORE_PREFIX + BUCKET.key(), String.valueOf(BUCKET.defaultValue() + 1));

        Arguments arg4 =
                Arguments.of(sessionOptions, enrichedOptions, generateTablePath(enrichedOptions));
        return Stream.of(arg1, arg2, arg3, arg4);
    }

    private static Map<String, String> generateTablePath(Map<String, String> enrichedOptions) {
        Map<String, String> expected = new HashMap<>(enrichedOptions);
        String rootPath = expected.remove(ROOT_PATH.key());
        if (rootPath != null) {
            String path =
                    rootPath + "/" + TableStoreManagedFactory.relativeTablePath(TABLE_IDENTIFIER);
            expected.put(PATH.key(), path);
        }
        return expected;
    }

    private static Stream<Arguments> provideOptionsToCreate() {
        Map<String, String> enrichedOptions =
                of(ROOT_PATH.key(), sharedTempDir.toAbsolutePath().toString());
        enrichedOptions = generateTablePath(enrichedOptions);
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Stream<Arguments> provideOptionsToDrop() {
        File tablePath =
                Paths.get(
                                sharedTempDir.toAbsolutePath().toString(),
                                TABLE_IDENTIFIER.asSummaryString())
                        .toFile();
        if (!tablePath.exists()) {
            tablePath.mkdirs();
        }
        Map<String, String> enrichedOptions =
                of(ROOT_PATH.key(), sharedTempDir.toAbsolutePath().toString());
        enrichedOptions = generateTablePath(enrichedOptions);
        return Stream.of(
                Arguments.of(enrichedOptions, false),
                Arguments.of(enrichedOptions, true),
                Arguments.of(enrichedOptions, false));
    }

    private static Stream<Arguments> provideResolvedTable() {
        RowType rowType = DEFAULT_ROW_TYPE;
        // success case
        Arguments arg0 =
                Arguments.of(
                        rowType,
                        DEFAULT_PART_TYPE.getFieldNames(), // partition is [dt, hr]
                        getPrimaryKeys(MULTI_PARTITIONED), // pk is [dt, hr, shopId]
                        new TableStoreTestBase.ExpectedResult().success(true));

        // failed case: pk doesn't contain partition key
        Arguments arg1 =
                Arguments.of(
                        rowType,
                        DEFAULT_PART_TYPE.getFieldNames(), // partition is [dt, hr]
                        singletonList("shopId"), // pk is [shopId]
                        new TableStoreTestBase.ExpectedResult()
                                .success(false)
                                .expectedType(IllegalStateException.class)
                                .expectedMessage(
                                        "Primary key constraint [shopId] should include all partition fields [dt, hr]"));

        // failed case: pk is same as partition key
        Arguments arg2 =
                Arguments.of(
                        rowType,
                        DEFAULT_PART_TYPE.getFieldNames(), // partition is [dt, hr]
                        DEFAULT_PART_TYPE.getFieldNames(), // pk is [dt, hr]
                        new TableStoreTestBase.ExpectedResult()
                                .success(false)
                                .expectedType(IllegalStateException.class)
                                .expectedMessage(
                                        "Primary key constraint [dt, hr] should not be same with partition fields [dt, hr],"
                                                + " this will result in only one record in a partition"));

        return Stream.of(arg0, arg1, arg2);
    }

    private static Map<String, String> addPrefix(
            Map<String, String> options, String prefix, Predicate<String> predicate) {
        Map<String, String> newOptions = new HashMap<>();
        options.forEach(
                (k, v) -> {
                    if (predicate.test(k)) {
                        newOptions.put(prefix + k, v);
                    }
                });
        return newOptions;
    }

    private static Map<String, String> of(String... kvs) {
        assert kvs != null && kvs.length % 2 == 0;
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length - 1; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }
}
