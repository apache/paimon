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

import static org.assertj.core.api.Assertions.assertThat;

/** File meta tests for column type evolution in {@link PrimaryKeyFileStoreTable}. */
public class PrimaryKeyTableColumnTypeFileMetaTest extends ColumnTypeFileMetaTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
    }

    @Override
    protected FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas) {
        SchemaManager schemaManager = new TestingSchemaManager(tablePath, tableSchemas);
        return new PrimaryKeyFileStoreTable(fileIO, tablePath, schemaManager.latest().get()) {
            @Override
            public SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }

    @Override
    protected SimpleStats getTableValueStats(DataFileMeta fileMeta) {
        return fileMeta.keyStats();
    }

    /** We can only validate col stats of primary keys in changelog with key table. */
    @Override
    protected void validateStatsField(List<DataFileMeta> fileMetaList) {
        for (DataFileMeta fileMeta : fileMetaList) {
            SimpleStats stats = getTableValueStats(fileMeta);
            assertThat(stats.maxValues().getFieldCount()).isEqualTo(4);
            for (int i = 0; i < 4; i++) {
                assertThat(stats.minValues().isNullAt(i)).isFalse();
                assertThat(stats.maxValues().isNullAt(i)).isFalse();
            }
        }
    }

    @Override
    @Test
    public void testTableSplitFilterNormalFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(6, 200L, 500L);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    // filter value by whole bucket
                    checkFilterRowCount(toDataFileMetas(splits), 2L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .between(6, 200F, 500F))
                                    .read()
                                    .dataSplits();
                    // filter g: old file cannot apply filter, so the new file in the same partition
                    // also cannot be filtered, the result contains all data
                    checkFilterRowCount(toDataFileMetas(splits), 6L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }
}
