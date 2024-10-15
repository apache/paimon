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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
            protected SchemaManager schemaManager() {
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
                    // filtered and only 3 rows left
                    checkFilterRowCount(toDataFileMetas(splits), 3L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    /** We can only validate the values in primary keys for changelog with key table. */
    @Override
    protected void validateValuesWithNewSchema(
            Map<Long, TableSchema> tableSchemas,
            long schemaId,
            List<String> filesName,
            List<DataFileMeta> fileMetaList) {
        Function<Long, List<DataField>> schemaFields =
                id -> tableSchemas.get(id).logicalTrimmedPrimaryKeysType().getFields();
        SimpleStatsEvolutions converters = new SimpleStatsEvolutions(schemaFields, schemaId);
        for (DataFileMeta fileMeta : fileMetaList) {
            SimpleStats stats = getTableValueStats(fileMeta);
            SimpleStatsEvolution.Result result =
                    converters.getOrCreate(fileMeta.schemaId()).evolution(stats, null, null);
            InternalRow min = result.minValues();
            InternalRow max = result.maxValues();
            assertThat(min.getFieldCount()).isEqualTo(4);
            if (filesName.contains(fileMeta.fileName())) {
                // parquet does not support padding
                assertThat(min.getString(0).toString()).startsWith("200");
                assertThat(max.getString(0).toString()).startsWith("300");

                assertThat(min.getString(1)).isEqualTo(BinaryString.fromString("201"));
                assertThat(max.getString(1)).isEqualTo(BinaryString.fromString("301"));

                assertThat(min.getDouble(2)).isEqualTo(202D);
                assertThat(max.getDouble(2)).isEqualTo(302D);

                assertThat(min.getInt(3)).isEqualTo(203);
                assertThat(max.getInt(3)).isEqualTo(303);
            } else {
                assertThat(min.getString(0))
                        .isEqualTo(max.getString(0))
                        .isEqualTo(BinaryString.fromString("400"));
                assertThat(min.getString(1))
                        .isEqualTo(max.getString(1))
                        .isEqualTo(BinaryString.fromString("401"));
                assertThat(min.getDouble(2)).isEqualTo(max.getDouble(2)).isEqualTo(402D);
                assertThat(min.getInt(3)).isEqualTo(max.getInt(3)).isEqualTo(403);
            }
        }
    }
}
