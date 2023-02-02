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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.utils.FailingFileIO;
import org.apache.flink.table.store.fs.FileIOFinder;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DoubleType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;

import org.junit.jupiter.api.AfterEach;
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

import static org.apache.flink.table.store.file.utils.FailingFileIO.retryArtificialException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link SchemaManager}. */
public class SchemaManagerTest {

    @TempDir java.nio.file.Path tempDir;

    private SchemaManager manager;

    private final List<String> partitionKeys = Collections.singletonList("f0");
    private final List<String> primaryKeys = Arrays.asList("f0", "f1");
    private final Map<String, String> options = Collections.singletonMap("key", "value");
    private final RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
    private final UpdateSchema updateSchema =
            new UpdateSchema(rowType, partitionKeys, primaryKeys, options, "");

    @BeforeEach
    public void beforeEach() throws IOException {
        // for failure tests
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 100, 100);
        String root = FailingFileIO.getFailingPath(failingName, tempDir.toString());
        Path path = new Path(root);
        manager = new SchemaManager(FileIOFinder.find(path), path);
    }

    @AfterEach
    public void afterEach() {
        // assert no temp file
        File schema = new File(tempDir.toFile(), "schema");
        assertThat(schema.exists()).isTrue();
        String[] versions = schema.list();
        assertThat(versions).isNotNull();
        for (String version : versions) {
            assertThat(version.startsWith(".")).isFalse();
        }
    }

    @Test
    public void testCommitNewVersion() throws Exception {
        TableSchema tableSchema =
                retryArtificialException(() -> manager.commitNewVersion(updateSchema));

        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());

        List<DataField> fields =
                Arrays.asList(
                        new DataField(
                                0, "f0", new org.apache.flink.table.store.types.IntType(false)),
                        new DataField(
                                1, "f1", new org.apache.flink.table.store.types.BigIntType(false)),
                        new DataField(
                                2, "f2", new org.apache.flink.table.store.types.VarCharType()));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);

        assertThatThrownBy(
                        () ->
                                retryArtificialException(
                                        () ->
                                                manager.commitNewVersion(
                                                        new UpdateSchema(
                                                                rowType,
                                                                partitionKeys,
                                                                primaryKeys,
                                                                Collections.singletonMap(
                                                                        CoreOptions.SEQUENCE_FIELD
                                                                                .key(),
                                                                        "f4"),
                                                                ""))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Nonexistent sequence field: 'f4'");
    }

    @Test
    public void testUpdateOptions() throws Exception {
        retryArtificialException(() -> manager.commitNewVersion(updateSchema));
        Map<String, String> newOptions = Collections.singletonMap("new_k", "new_v");
        UpdateSchema updateSchema =
                new UpdateSchema(rowType, partitionKeys, primaryKeys, newOptions, "my_comment_2");
        retryArtificialException(() -> manager.commitNewVersion(updateSchema));
        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());
        assertThat(latest.isPresent()).isTrue();
        assertThat(latest.get().options()).isEqualTo(newOptions);
    }

    @Test
    public void testUpdateSchema() throws Exception {
        retryArtificialException(() -> manager.commitNewVersion(updateSchema));
        RowType newType = RowType.of(new IntType(), new BigIntType());
        UpdateSchema updateSchema =
                new UpdateSchema(newType, partitionKeys, primaryKeys, options, "my_comment_3");
        assertThrows(
                UnsupportedOperationException.class,
                () -> retryArtificialException(() -> manager.commitNewVersion(updateSchema)));
    }

    @Test
    public void testConcurrentCommit() throws Exception {
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

                                Map<String, String> options = new HashMap<>();
                                options.put("id", String.valueOf(id));
                                UpdateSchema updateSchema =
                                        new UpdateSchema(
                                                rowType,
                                                partitionKeys,
                                                primaryKeys,
                                                options,
                                                "my_comment_4");
                                try {
                                    retryArtificialException(
                                            () -> manager.commitNewVersion(updateSchema));
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
                        IntStream.range(0, threadNumber)
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
        final UpdateSchema mapPrimaryKeySchema =
                new UpdateSchema(mapPrimaryKeyType, partitionKeys, primaryKeys, options, "");
        assertThatThrownBy(() -> manager.commitNewVersion(mapPrimaryKeySchema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "The type %s in primary key field %s is unsupported",
                        MapType.class.getSimpleName(), "f0");

        RowType doublePrimaryKeyType =
                RowType.of(new DoubleType(), new BigIntType(), new VarCharType());
        final UpdateSchema doublePrimaryKeySchema =
                new UpdateSchema(doublePrimaryKeyType, partitionKeys, primaryKeys, options, "");

        TableSchema tableSchema =
                retryArtificialException(() -> manager.commitNewVersion(doublePrimaryKeySchema));

        Optional<TableSchema> latest = retryArtificialException(() -> manager.latest());

        List<DataField> fields =
                Arrays.asList(
                        new DataField(
                                0, "f0", new org.apache.flink.table.store.types.DoubleType(false)),
                        new DataField(
                                1, "f1", new org.apache.flink.table.store.types.BigIntType(false)),
                        new DataField(
                                2, "f2", new org.apache.flink.table.store.types.VarCharType()));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);
    }

    @Test
    public void testChangelogTableWithFullCompaction() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("key", "value");
        options.put(
                CoreOptions.CHANGELOG_PRODUCER.key(),
                CoreOptions.ChangelogProducer.FULL_COMPACTION.toString());

        final UpdateSchema schemaWithoutPrimaryKeys =
                new UpdateSchema(
                        rowType, Collections.EMPTY_LIST, Collections.EMPTY_LIST, options, "");
        assertThatThrownBy(() -> manager.commitNewVersion(schemaWithoutPrimaryKeys))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Changelog table with full compaction must have primary keys");

        final UpdateSchema schemaWithPrimaryKeys =
                new UpdateSchema(rowType, partitionKeys, primaryKeys, options, "");
        retryArtificialException(() -> manager.commitNewVersion(schemaWithPrimaryKeys));
    }

    @Test
    public void testAppendOnlyTableWithPrimaryKey() throws Exception {
        RowType newType = RowType.of(new IntType(), new BigIntType());
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_MODE.key(), WriteMode.CHANGE_LOG.toString());
        UpdateSchema changeLogSchema =
                new UpdateSchema(newType, partitionKeys, primaryKeys, options, "change-log table");
        retryArtificialException(() -> manager.commitNewVersion(changeLogSchema));

        options.put(CoreOptions.WRITE_MODE.key(), WriteMode.APPEND_ONLY.toString());
        UpdateSchema updateSchema =
                new UpdateSchema(
                        newType,
                        partitionKeys,
                        primaryKeys,
                        options,
                        "append-only table with primary key");
        assertThatThrownBy(
                        () ->
                                retryArtificialException(
                                        () -> manager.commitNewVersion(updateSchema)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        "Cannot define any primary key in an append-only table. "
                                + "Set 'write-mode'='change-log' if still want to keep the primary key definition.");
    }
}
