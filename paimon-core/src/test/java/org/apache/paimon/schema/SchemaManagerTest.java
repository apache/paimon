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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.FailingFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SchemaManager}. */
public class SchemaManagerTest {

    @TempDir java.nio.file.Path tempDir;

    private SchemaManager manager;
    private Path path;

    private final List<String> partitionKeys = Collections.singletonList("f0");
    private final List<String> primaryKeys = Arrays.asList("f0", "f1");
    private final Map<String, String> options = Collections.singletonMap("key", "value");
    private final RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
    private final Schema schema =
            new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, "");

    @BeforeEach
    public void beforeEach() throws IOException {
        // for failure tests
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 100, 100);
        String root = FailingFileIO.getFailingPath(failingName, tempDir.toString());
        path = new Path(root);
        manager = new SchemaManager(FileIOFinder.find(path), path);
    }

    @AfterEach
    public void afterEach() {
        // assert no temp file
        File schema = new File(tempDir.toFile(), "schema");
        if (schema.exists()) {
            String[] versions = schema.list();
            assertThat(versions).isNotNull();
            for (String version : versions) {
                assertThat(version.startsWith(".")).isFalse();
            }
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        TableSchema tableSchema = retryArtificialException(() -> manager.createTable(schema));

        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new org.apache.paimon.types.IntType(false)),
                        new DataField(1, "f1", new org.apache.paimon.types.BigIntType(false)),
                        new DataField(2, "f2", new org.apache.paimon.types.VarCharType()));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);
    }

    @Test
    public void testCreateTableIllegal() {
        assertThatThrownBy(
                        () ->
                                retryArtificialException(
                                        () ->
                                                manager.createTable(
                                                        new Schema(
                                                                rowType.getFields(),
                                                                partitionKeys,
                                                                primaryKeys,
                                                                Collections.singletonMap(
                                                                        CoreOptions.SEQUENCE_FIELD
                                                                                .key(),
                                                                        "f4"),
                                                                ""))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Sequence field: 'f4' can not be found in table schema.");
    }

    @Test
    public void testUpdateOptions() throws Exception {
        retryArtificialException(() -> manager.createTable(this.schema));
        retryArtificialException(
                () -> manager.commitChanges(SchemaChange.setOption("new_k", "new_v")));
        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());
        assertThat(latest.isPresent()).isTrue();
        assertThat(latest.get().options()).containsEntry("new_k", "new_v");
    }

    @Test
    public void testConcurrentCommit() throws Exception {
        retryArtificialException(
                () ->
                        manager.createTable(
                                new Schema(
                                        rowType.getFields(),
                                        partitionKeys,
                                        primaryKeys,
                                        Collections.singletonMap("id", "-1"),
                                        "my_comment_4")));

        int threadNumber = ThreadLocalRandom.current().nextInt(3) + 2;
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadNumber; i++) {
            int id = i;
            threads.add(
                    new Thread(
                            () -> {
                                // sleep to concurrent commit, exclude the effects of thread startup
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }

                                try {
                                    retryArtificialException(
                                            () ->
                                                    manager.commitChanges(
                                                            SchemaChange.setOption(
                                                                    "id", String.valueOf(id))));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // assert ids
        // use set, possible duplicate committing
        Set<String> ids =
                retryArtificialException(() -> manager.listAll()).stream()
                        .map(schema -> schema.options().get("id"))
                        .collect(Collectors.toSet());
        assertThat(ids)
                .containsExactlyInAnyOrder(
                        IntStream.range(-1, threadNumber)
                                .mapToObj(String::valueOf)
                                .toArray(String[]::new));
    }

    @Test
    public void testPrimaryKeyType() throws Exception {
        final RowType mapPrimaryKeyType =
                RowType.of(
                        new MapType(new IntType(), new BigIntType()),
                        new BigIntType(),
                        new VarCharType());
        final Schema mapPrimaryKeySchema =
                new Schema(mapPrimaryKeyType.getFields(), partitionKeys, primaryKeys, options, "");
        assertThatThrownBy(() -> manager.createTable(mapPrimaryKeySchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The type %s in primary key field %s is unsupported",
                        MapType.class.getSimpleName(), "f0");

        RowType doublePrimaryKeyType =
                RowType.of(new DoubleType(), new BigIntType(), new VarCharType());
        final Schema doublePrimaryKeySchema =
                new Schema(
                        doublePrimaryKeyType.getFields(), partitionKeys, primaryKeys, options, "");

        TableSchema tableSchema =
                retryArtificialException(() -> manager.createTable(doublePrimaryKeySchema));

        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new org.apache.paimon.types.DoubleType(false)),
                        new DataField(1, "f1", new org.apache.paimon.types.BigIntType(false)),
                        new DataField(2, "f2", new org.apache.paimon.types.VarCharType()));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);
    }

    @Test
    public void testPartitionType() {
        final RowType mapPrimaryKeyType =
                RowType.of(
                        new MapType(new IntType(), new BigIntType()),
                        new BigIntType(),
                        new VarCharType());
        final Schema mapPartitionSchema =
                new Schema(
                        mapPrimaryKeyType.getFields(),
                        partitionKeys,
                        Collections.emptyList(),
                        options,
                        "");
        assertThatThrownBy(() -> manager.createTable(mapPartitionSchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The type %s in partition field %s is unsupported",
                        MapType.class.getSimpleName(), "f0");
    }

    @Test
    public void testChangelogTableWithFullCompaction() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("key", "value");
        options.put(
                CoreOptions.CHANGELOG_PRODUCER.key(),
                CoreOptions.ChangelogProducer.FULL_COMPACTION.toString());

        final Schema schemaWithPrimaryKeys =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, options, "");
        retryArtificialException(() -> manager.createTable(schemaWithPrimaryKeys));
    }

    @Test
    public void testDeleteSchemaWithSchemaId() throws Exception {
        Map<String, String> options = new HashMap<>();
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        partitionKeys,
                        primaryKeys,
                        options,
                        "append-only table with primary key");
        // use non-failing manager
        SchemaManager manager = new SchemaManager(LocalFileIO.create(), path);
        manager.createTable(schema);
        String schemaContent = manager.latest().get().toString();

        manager.commitChanges(SchemaChange.setOption("aa", "bb"));
        assertThat(manager.latest().get().options().get("aa")).isEqualTo("bb");

        manager.deleteSchema(manager.latest().get().id());
        assertThat(manager.latest().get().toString()).isEqualTo(schemaContent);
    }

    @Test
    public void testApplyMove() {
        // Create the initial list of fields
        List<DataField> fields =
                new ArrayList<>(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING())));

        // Use factory methods to create Move objects
        SchemaChange.Move moveFirst = SchemaChange.Move.first("f2");
        SchemaChange.Move moveLast = SchemaChange.Move.last("f0");
        SchemaChange.Move moveAfter = SchemaChange.Move.after("f0", "f1");
        SchemaChange.Move moveBefore = SchemaChange.Move.before("f2", "f1");

        // Test FIRST operation
        manager.applyMove(fields, moveFirst);
        Assertions.assertEquals(
                2,
                fields.get(0).id(),
                "The field id should remain as 2 after moving f2 to the first position");
        Assertions.assertEquals(
                fields.get(0).name(), "f2 should be moved to the first position", "f2");

        // Reset fields to initial state
        fields =
                new ArrayList<>(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING())));

        // Test LAST operation
        manager.applyMove(fields, moveLast);
        Assertions.assertEquals(
                0,
                fields.get(fields.size() - 1).id(),
                "The field id should remain as 0 after moving f0 to the last position");
        Assertions.assertEquals(
                "f0",
                fields.get(fields.size() - 1).name(),
                "f0 should be moved to the last position");

        // Reset fields to initial state
        fields =
                new ArrayList<>(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING())));

        // Test AFTER operation
        manager.applyMove(fields, moveAfter);
        Assertions.assertEquals(
                0, fields.get(2).id(), "The field id should remain as 0 after moving f0 after f1");
        Assertions.assertEquals("f0", fields.get(2).name(), "f0 should be after f1");

        // Reset fields to initial state
        fields =
                new ArrayList<>(
                        Arrays.asList(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.BIGINT()),
                                new DataField(2, "f2", DataTypes.STRING())));

        // Test BEFORE operation
        manager.applyMove(fields, moveBefore);
        Assertions.assertEquals(
                2, fields.get(1).id(), "The field id should remain as 2 after moving f2 before f1");
        Assertions.assertEquals("f2", fields.get(1).name(), "f2 should be before f1");
    }
}
