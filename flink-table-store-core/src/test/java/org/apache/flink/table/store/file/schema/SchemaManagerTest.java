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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

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

import static org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem.retryArtificialException;
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
        FailingAtomicRenameFileSystem.reset(failingName, 100, 100);
        String root = FailingAtomicRenameFileSystem.getFailingPath(failingName, tempDir.toString());
        manager = new SchemaManager(new Path(root));
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
                        new DataField(0, "f0", new AtomicDataType(new IntType(false))),
                        new DataField(1, "f1", new AtomicDataType(new BigIntType(false))),
                        new DataField(2, "f2", new AtomicDataType(new VarCharType())));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);

        assertThatThrownBy(
                        () ->
                                manager.commitNewVersion(
                                        new UpdateSchema(
                                                rowType,
                                                partitionKeys,
                                                primaryKeys,
                                                Collections.singletonMap(
                                                        CoreOptions.SEQUENCE_FIELD.key(), "f4"),
                                                "")))
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
                        new DataField(0, "f0", new AtomicDataType(new DoubleType(false))),
                        new DataField(1, "f1", new AtomicDataType(new BigIntType(false))),
                        new DataField(2, "f2", new AtomicDataType(new VarCharType())));

        assertThat(latest.isPresent()).isTrue();
        assertThat(tableSchema).isEqualTo(latest.get());
        assertThat(tableSchema.fields()).isEqualTo(fields);
        assertThat(tableSchema.partitionKeys()).isEqualTo(partitionKeys);
        assertThat(tableSchema.primaryKeys()).isEqualTo(primaryKeys);
        assertThat(tableSchema.options()).isEqualTo(options);
    }
}
