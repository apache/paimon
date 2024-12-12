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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Tests for meta files in {@link PrimaryKeyFileStoreTable} with schema evolution. */
public class PrimaryKeyFileMetaFilterTest extends FileMetaFilterTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
    }

    @Test
    @Override
    public void testTableSplit() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 6L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 12L);

                    /**
                     * TODO PrimaryKeyFileStoreTable doesn't support value predicate and can't get
                     * value stats. The test for filtering the primary key and partition already
                     * exists.
                     */
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    @Override
    public void testTableSplitFilterExistFields() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // results of field "b" in [14, 19] in SCHEMA_0_FIELDS, "b" is renamed to "d" in
                    // SCHEMA_1_FIELDS
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(2, 14, 19);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 6L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());
                    // results of field "d" in [14, 19] in SCHEMA_1_FIELDS
                    Predicate predicate = builder.between(1, 14, 19);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 12L);

                    /**
                     * TODO PrimaryKeyFileStoreTable doesn't support value predicate and can't get
                     * value stats. The test for filtering the primary key and partition already
                     * exists.
                     */
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    @Override
    public void testTableSplitFilterNewFields() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 6L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());
                    // results of field "a" in (1120, -] in SCHEMA_1_FIELDS, "a" is not existed in
                    // SCHEMA_0_FIELDS
                    Predicate predicate = builder.greaterOrEqual(3, 1120);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 12L);

                    predicate = builder.greaterThan(3, 1120);
                    splits = table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    // filtered the whole bucket
                    checkFilterRowCount(toDataFileMetas(splits), 7L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas) {
        SchemaManager schemaManager = new TestingSchemaManager(tablePath, tableSchemas);
        return new PrimaryKeyFileStoreTable(
                fileIO, tablePathProvider, schemaManager.latest().get()) {
            @Override
            public SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }

    @Override
    protected SimpleStats getTableValueStats(DataFileMeta fileMeta) {
        throw new UnsupportedOperationException();
    }
}
