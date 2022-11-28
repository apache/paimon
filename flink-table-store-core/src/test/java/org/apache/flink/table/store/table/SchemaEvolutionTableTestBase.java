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

package org.apache.flink.table.store.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.schema.AtomicDataType;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.TestAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.TraceableFileSystem;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for schema evolution in {@link FileStoreTable}. */
public abstract class SchemaEvolutionTableTestBase {
    protected static final List<DataField> SCHEMA_0_FIELDS =
            Arrays.asList(
                    new DataField(0, "a", new AtomicDataType(DataTypes.STRING().getLogicalType())),
                    new DataField(1, "pt", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(2, "b", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(3, "c", new AtomicDataType(DataTypes.STRING().getLogicalType())),
                    new DataField(4, "kt", new AtomicDataType(DataTypes.BIGINT().getLogicalType())),
                    new DataField(5, "d", new AtomicDataType(DataTypes.STRING().getLogicalType())));
    protected static final List<DataField> SCHEMA_1_FIELDS =
            Arrays.asList(
                    new DataField(1, "pt", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(2, "d", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(4, "kt", new AtomicDataType(DataTypes.BIGINT().getLogicalType())),
                    new DataField(6, "a", new AtomicDataType(DataTypes.INT().getLogicalType())),
                    new DataField(7, "f", new AtomicDataType(DataTypes.STRING().getLogicalType())),
                    new DataField(8, "b", new AtomicDataType(DataTypes.STRING().getLogicalType())));
    protected static final List<String> PARTITION_NAMES = Collections.singletonList("pt");
    protected static final List<String> PRIMARY_KEY_NAMES = Arrays.asList("pt", "kt");

    protected Path tablePath;
    protected String commitUser;
    protected final Configuration tableConfig = new Configuration();

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new Path(TestAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();
        tableConfig.set(CoreOptions.PATH, tablePath.toString());
        tableConfig.set(CoreOptions.BUCKET, 2);
    }

    @AfterEach
    public void after() throws IOException {
        // assert all connections are closed
        FileSystem fileSystem = tablePath.getFileSystem();
        assertThat(fileSystem).isInstanceOf(TraceableFileSystem.class);
        TraceableFileSystem traceableFileSystem = (TraceableFileSystem) fileSystem;

        java.util.function.Predicate<Path> pathPredicate =
                path -> path.toString().contains(tempDir.toString());
        assertThat(traceableFileSystem.openInputStreams(pathPredicate)).isEmpty();
        assertThat(traceableFileSystem.openOutputStreams(pathPredicate)).isEmpty();
    }

    protected List<String> getPrimaryKeyNames() {
        return PRIMARY_KEY_NAMES;
    }

    protected abstract FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas);

    public static <R> void writeAndCheckFileResult(
            Function<Map<Long, TableSchema>, R> firstChecker,
            BiConsumer<R, Map<Long, TableSchema>> secondChecker,
            List<String> primaryKeyNames,
            Configuration tableConfig,
            Function<Map<Long, TableSchema>, FileStoreTable> createFileStoreTable)
            throws Exception {
        Map<Long, TableSchema> tableSchemas = new HashMap<>();
        // Create schema with SCHEMA_0_FIELDS
        tableSchemas.put(
                0L,
                new TableSchema(
                        0,
                        SCHEMA_0_FIELDS,
                        5,
                        PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        FileStoreTable table = createFileStoreTable.apply(tableSchemas);
        TableWrite write = table.newWrite("user");
        TableCommit commit = table.newCommit("user");

        write.write(rowData("S001", 1, 11, "S11", 111L, "S111"));
        write.write(rowData("S002", 2, 12, "S12", 112L, "S112"));
        write.write(rowData("S003", 1, 13, "S13", 113L, "S113"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData("S004", 1, 14, "S14", 114L, "S114"));
        write.write(rowData("S005", 2, 15, "S15", 115L, "S115"));
        write.write(rowData("S006", 2, 16, "S16", 116L, "S116"));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        R result = firstChecker.apply(tableSchemas);

        /**
         * The table fields are: 0->a, 1->pt, 2->b, 3->c, 4->kt, 5->d. We will alter the table as
         * follows:
         *
         * <ul>
         *   <li>1. delete fields "a", "c" and "d"
         *   <li>2. rename "b" to "d"
         *   <li>3. add new fields "a", "f", "b"
         * </ul>
         *
         * <p>The result table fields will be: 1->pt, 2->d, 4->kt, 6->a, 7->f, 8->b.
         */
        tableSchemas.put(
                1L,
                new TableSchema(
                        1,
                        SCHEMA_1_FIELDS,
                        8,
                        PARTITION_NAMES,
                        primaryKeyNames,
                        tableConfig.toMap(),
                        ""));
        table = createFileStoreTable.apply(tableSchemas);
        write = table.newWrite("user");
        commit = table.newCommit("user");

        write.write(rowData(1, 17, 117L, 1117, "S007", "S17"));
        write.write(rowData(2, 18, 118L, 1118, "S008", "S18"));
        write.write(rowData(1, 19, 119L, 1119, "S009", "S19"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 20, 120L, 1120, "S010", "S20"));
        write.write(rowData(1, 21, 121L, 1121, "S011", "S21"));
        write.write(rowData(1, 22, 122L, 1122, "S012", "S22"));
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();

        secondChecker.accept(result, tableSchemas);
    }

    protected static RowData rowData(Object... values) {
        List<Object> valueList = new ArrayList<>(values.length);
        for (Object value : values) {
            if (value instanceof String) {
                valueList.add(StringData.fromString((String) value));
            } else {
                valueList.add(value);
            }
        }
        return GenericRowData.of(valueList.toArray(new Object[0]));
    }

    /** {@link SchemaManager} subclass for testing. */
    public static class TestingSchemaManager extends SchemaManager {
        private final Map<Long, TableSchema> tableSchemas;

        public TestingSchemaManager(Path tableRoot, Map<Long, TableSchema> tableSchemas) {
            super(tableRoot);
            this.tableSchemas = tableSchemas;
        }

        @Override
        public Optional<TableSchema> latest() {
            return Optional.of(
                    tableSchemas.get(
                            tableSchemas.keySet().stream()
                                    .max(Long::compareTo)
                                    .orElseThrow(IllegalStateException::new)));
        }

        @Override
        public List<TableSchema> listAll() {
            return new ArrayList<>(tableSchemas.values());
        }

        @Override
        public List<Long> listAllIds() {
            return new ArrayList<>(tableSchemas.keySet());
        }

        @Override
        public TableSchema commitNewVersion(UpdateSchema updateSchema) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema commitChanges(List<SchemaChange> changes) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableSchema schema(long id) {
            return checkNotNull(tableSchemas.get(id));
        }
    }
}
