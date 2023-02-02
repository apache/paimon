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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.table.source.DataTableScan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** File meta tests for column type evolution in {@link ChangelogWithKeyFileStoreTable}. */
public class ChangelogWithKeyTableColumnTypeFileMetaTest extends ColumnTypeFileMetaTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
        tableConfig.set(CoreOptions.WRITE_MODE, WriteMode.CHANGE_LOG);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas) {
        SchemaManager schemaManager = new TestingSchemaManager(tablePath, tableSchemas);
        return new ChangelogWithKeyFileStoreTable(fileIO, tablePath, schemaManager.latest().get()) {
            @Override
            protected SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }

    @Override
    protected BinaryTableStats getTableValueStats(DataFileMeta fileMeta) {
        return fileMeta.keyStats();
    }

    /** We can only validate field stats of primary keys in changelog with key table. */
    @Override
    protected void validateStatsField(List<DataFileMeta> fileMetaList) {
        for (DataFileMeta fileMeta : fileMetaList) {
            FieldStats[] statsArray = getTableValueStats(fileMeta).fields(null);
            assertThat(statsArray.length).isEqualTo(4);
            for (int i = 0; i < 4; i++) {
                assertThat(statsArray[i].minValue()).isNotNull();
                assertThat(statsArray[i].maxValue()).isNotNull();
            }
        }
    }

    @Override
    @Test
    public void testTableScanFilterNormalFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    /**
                     * Changelog with key table doesn't support filter in value, it will scan all
                     * data. TODO support filter value in future.
                     */
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(6, 200L, 500L);
                    DataTableScan.DataFilePlan plan = table.newScan().withFilter(predicate).plan();
                    checkFilterRowCount(plan, 3L);
                    return plan.splits.stream()
                            .flatMap(s -> s.files().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    /**
                     * Changelog with key table doesn't support filter in value, it will scan all
                     * data. TODO support filter value in future.
                     */
                    DataTableScan.DataFilePlan plan =
                            table.newScan()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .between(6, 200F, 500F))
                                    .plan();
                    checkFilterRowCount(plan, 6L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    /** We can only validate the values in primary keys for changelog with key table. */
    @Override
    protected void validateValuesWithNewSchema(
            List<String> filesName, List<DataFileMeta> fileMetaList) {
        for (DataFileMeta fileMeta : fileMetaList) {
            FieldStats[] statsArray = getTableValueStats(fileMeta).fields(null);
            assertThat(statsArray.length).isEqualTo(4);
            if (filesName.contains(fileMeta.fileName())) {
                assertThat(statsArray[0].minValue())
                        .isEqualTo(BinaryString.fromString("200       "));
                assertThat(statsArray[0].maxValue())
                        .isEqualTo(BinaryString.fromString("300       "));

                assertThat(statsArray[1].minValue()).isEqualTo(BinaryString.fromString("201"));
                assertThat(statsArray[1].maxValue()).isEqualTo(BinaryString.fromString("301"));

                assertThat((Double) statsArray[2].minValue()).isEqualTo(202D);
                assertThat((Double) statsArray[2].maxValue()).isEqualTo(302D);

                assertThat((Integer) statsArray[3].minValue()).isEqualTo(203);
                assertThat((Integer) statsArray[3].maxValue()).isEqualTo(303);
            } else {
                assertThat(statsArray[0].minValue())
                        .isEqualTo(statsArray[0].maxValue())
                        .isEqualTo(BinaryString.fromString("400"));
                assertThat(statsArray[1].minValue())
                        .isEqualTo(statsArray[1].maxValue())
                        .isEqualTo(BinaryString.fromString("401"));
                assertThat((Double) statsArray[2].minValue())
                        .isEqualTo((Double) statsArray[2].maxValue())
                        .isEqualTo(402D);
                assertThat((Integer) statsArray[3].minValue())
                        .isEqualTo(statsArray[3].maxValue())
                        .isEqualTo(403);
            }
        }
    }
}
