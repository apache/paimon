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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests fail-closed refresh, uniqueness, and concurrent access of materialized query state. */
public class DataEvolutionGlobalIndexTableQueryTest extends TableTestBase {

    @Test
    public void testRefreshAndDuplicatePolicy() throws Exception {
        FileStoreTable table = createAppendTable();
        try (DataEvolutionGlobalIndexTableQuery query = newQuery(table)) {
            BinaryRow key = key("https://example/a");
            BinaryRow value = value("descriptor-a");

            assertThatThrownBy(
                            () ->
                                    query.lookup(
                                            BinaryRow.EMPTY_ROW,
                                            0,
                                            GenericRow.of(
                                                    BinaryString.fromString("https://example/a"))))
                    .isInstanceOf(QueryServiceNotReadyException.class);

            query.beginRefresh(1L, 10L);
            query.put(1L, key, value);
            query.finishRefresh(1L);
            assertThat(
                            query.lookup(
                                            BinaryRow.EMPTY_ROW,
                                            0,
                                            GenericRow.of(
                                                    BinaryString.fromString("https://example/a")))
                                    .getString(0)
                                    .toString())
                    .isEqualTo("descriptor-a");

            query.beginRefresh(2L, 11L);
            query.put(2L, key, value("first"));
            assertThatThrownBy(() -> query.put(2L, key, value("second")))
                    .isInstanceOf(DuplicateLookupKeyException.class)
                    .hasMessageContaining("configured as unique");
            query.markNotReady(2L, 11L, "duplicate key");
            assertThatThrownBy(() -> query.lookup(BinaryRow.EMPTY_ROW, 0, key))
                    .isInstanceOf(QueryServiceNotReadyException.class)
                    .hasMessageContaining("duplicate key");
            assertThat(query.ready()).isFalse();
            assertThat(query.latestGeneration()).isEqualTo(2L);
            assertThat(query.servedGeneration()).isEqualTo(1L);
            assertThat(query.servedSnapshotId()).isEqualTo(10L);

            query.beginRefresh(3L, 12L);
            query.put(3L, key, value("descriptor-new"));
            query.finishRefresh(3L);
            assertThat(query.lookup(BinaryRow.EMPTY_ROW, 0, key).getString(0).toString())
                    .isEqualTo("descriptor-new");
        }
    }

    @Test
    public void testConcurrentLookupIsSerializedSafely() throws Exception {
        FileStoreTable table = createAppendTable();
        try (DataEvolutionGlobalIndexTableQuery query = newQuery(table)) {
            BinaryRow key = key("https://example/concurrent");
            query.beginRefresh(1L, 1L);
            query.put(1L, key, value("descriptor"));
            query.finishRefresh(1L);

            ExecutorService executor = Executors.newFixedThreadPool(8);
            try {
                Callable<String> lookup =
                        () -> query.lookup(BinaryRow.EMPTY_ROW, 0, key).getString(0).toString();
                List<Future<String>> futures = executor.invokeAll(Collections.nCopies(200, lookup));
                for (Future<String> future : futures) {
                    assertThat(future.get()).isEqualTo("descriptor");
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void testSchemalessBinaryKeyOffsetIsNormalized() throws Exception {
        FileStoreTable table = createAppendTable();
        try (DataEvolutionGlobalIndexTableQuery query = newQuery(table)) {
            BinaryRow wireKey = deserializeBinaryRow(serializeBinaryRow(key("wire-key")));
            assertThat(wireKey.getOffset()).isNotZero();
            wireKey.setRowKind(RowKind.DELETE);
            assertThat(GlobalIndexQueryServiceUtils.route(wireKey, 17))
                    .isEqualTo(GlobalIndexQueryServiceUtils.route(key("wire-key"), 17));

            query.beginRefresh(1L, 1L);
            query.put(1L, wireKey, value("descriptor"));
            query.finishRefresh(1L);

            BinaryRow updateKey = key("wire-key");
            updateKey.setRowKind(RowKind.UPDATE_AFTER);
            assertThat(query.lookup(BinaryRow.EMPTY_ROW, 0, updateKey).getString(0).toString())
                    .isEqualTo("descriptor");
        }
    }

    @Test
    public void testOversizedValueCannotBecomeReady() throws Exception {
        FileStoreTable table = createAppendTable();
        try (DataEvolutionGlobalIndexTableQuery query = newQuery(table)) {
            BinaryRow oversized = new BinaryRow(1);
            oversized.pointTo(
                    MemorySegment.wrap(new byte[BinaryRow.calculateFixPartSizeInBytes(1)]),
                    0,
                    GlobalIndexQueryServiceUtils.MAX_TOTAL_VALUE_BYTES);

            query.beginRefresh(1L, 1L);
            assertThatThrownBy(() -> query.put(1L, key("too-large"), oversized))
                    .isInstanceOf(OversizedLookupValueException.class)
                    .hasMessageContaining("query protocol limit");
            assertThatThrownBy(() -> query.finishRefresh(1L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("oversized projected value");
            assertThat(query.ready()).isFalse();
        }
    }

    @Test
    public void testPersistedQueryAuthCannotBeDisabledDynamically() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d");
        options.put(CoreOptions.QUERY_AUTH_ENABLED.key(), "true");
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING())
                        .column("descriptor", DataTypes.STRING())
                        .options(options)
                        .build();
        catalog.createTable(identifier("AuthTable"), schema, false);
        FileStoreTable persistedAuth = (FileStoreTable) catalog.getTable(identifier("AuthTable"));
        FileStoreTable dynamicallyDisabled =
                (FileStoreTable)
                        persistedAuth.copy(
                                Collections.singletonMap(
                                        CoreOptions.QUERY_AUTH_ENABLED.key(), "false"));

        assertThatThrownBy(
                        () ->
                                GlobalIndexQueryServiceUtils.querySpec(
                                        dynamicallyDisabled,
                                        "url",
                                        Collections.singletonList("descriptor")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("persisted or dynamic query-auth.enabled");
    }

    @Test
    public void testOnlyRawScalarBlobValueFieldIsSupported() throws Exception {
        FileStoreTable descriptorTable =
                createBlobTable(
                        "DescriptorBlobTable",
                        "url",
                        DataTypes.STRING(),
                        "payload",
                        DataTypes.BLOB(),
                        CoreOptions.BLOB_DESCRIPTOR_FIELD.key(),
                        "payload");
        assertThatThrownBy(
                        () ->
                                GlobalIndexQueryServiceUtils.querySpec(
                                        descriptorTable,
                                        "url",
                                        Collections.singletonList("payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supports raw blob-file value field");

        FileStoreTable viewTable =
                createBlobTable(
                        "ViewBlobTable",
                        "url",
                        DataTypes.STRING(),
                        "payload",
                        DataTypes.BLOB(),
                        CoreOptions.BLOB_VIEW_FIELD.key(),
                        "payload");
        assertThatThrownBy(
                        () ->
                                GlobalIndexQueryServiceUtils.querySpec(
                                        viewTable, "url", Collections.singletonList("payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supports raw blob-file value field");

        FileStoreTable nestedTable =
                createBlobTable(
                        "NestedBlobTable",
                        "url",
                        DataTypes.STRING(),
                        "payload",
                        DataTypes.ARRAY(DataTypes.BLOB()),
                        CoreOptions.BLOB_FIELD.key(),
                        "payload");
        assertThatThrownBy(
                        () ->
                                GlobalIndexQueryServiceUtils.querySpec(
                                        nestedTable, "url", Collections.singletonList("payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support nested BLOB value field");

        FileStoreTable blobKeyTable =
                createBlobTable(
                        "BlobKeyTable",
                        "url",
                        DataTypes.BLOB(),
                        "payload",
                        DataTypes.STRING(),
                        CoreOptions.BLOB_FIELD.key(),
                        "url");
        assertThatThrownBy(
                        () ->
                                GlobalIndexQueryServiceUtils.querySpec(
                                        blobKeyTable, "url", Collections.singletonList("payload")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not support BLOB lookup field");
    }

    private FileStoreTable createAppendTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING().notNull())
                        .column("descriptor", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private FileStoreTable createBlobTable(
            String tableName,
            String keyName,
            DataType keyType,
            String valueName,
            DataType valueType,
            String blobOption,
            String blobField)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(keyName, keyType)
                        .column(valueName, valueType)
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .option(blobOption, blobField)
                        .build();
        catalog.createTable(identifier(tableName), schema, false);
        return (FileStoreTable) catalog.getTable(identifier(tableName));
    }

    private DataEvolutionGlobalIndexTableQuery newQuery(FileStoreTable table) {
        return new DataEvolutionGlobalIndexTableQuery(
                table,
                "url",
                Collections.singletonList("descriptor"),
                new File(tempPath.toFile(), "query-state-" + System.nanoTime()));
    }

    private BinaryRow key(String key) {
        InternalRowSerializer serializer =
                InternalSerializers.create(RowType.of(DataTypes.STRING().notNull()));
        return serializer.toBinaryRow(GenericRow.of(BinaryString.fromString(key))).copy();
    }

    private BinaryRow value(String value) {
        InternalRowSerializer serializer =
                InternalSerializers.create(RowType.of(DataTypes.STRING()));
        return serializer.toBinaryRow(GenericRow.of(BinaryString.fromString(value))).copy();
    }
}
