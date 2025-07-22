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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for column type evolution. */
public abstract class ColumnTypeFileMetaTestBase extends SchemaEvolutionTableTestBase {
    @BeforeEach
    public void before() throws Exception {
        super.before();
        tableConfig.set(CoreOptions.BUCKET, 1);
    }

    @Test
    public void testTableSplit() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 3L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // Scan all data files
                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .greaterOrEqual(
                                                            1, BinaryString.fromString("0")))
                                    .read()
                                    .dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 6L);

                    List<String> filesName =
                            files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
                    assertThat(filesName.size()).isGreaterThan(0);

                    List<DataFileMeta> fileMetaList =
                            splits.stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    assertThat(
                                    fileMetaList.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()))
                            .containsAll(filesName);

                    validateStatsField(fileMetaList);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    protected void validateStatsField(List<DataFileMeta> fileMetaList) {
        for (DataFileMeta fileMeta : fileMetaList) {
            SimpleStats stats = getTableValueStats(fileMeta);
            assertThat(stats.minValues().getFieldCount()).isEqualTo(12);
            for (int i = 0; i < 11; i++) {
                assertThat(stats.minValues().isNullAt(i)).isFalse();
                assertThat(stats.maxValues().isNullAt(i)).isFalse();
            }
            // Min and max value of binary type is null
            assertThat(stats.minValues().isNullAt(11)).isTrue();
            assertThat(stats.maxValues().isNullAt(11)).isTrue();
        }
    }

    @Test
    public void testTableSplitFilterNormalFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    /*
                     Filter field "g" in [200, 500] in SCHEMA_FIELDS which is bigint and will get
                     one file with two data as followed:

                     <ul>
                       <li>2,"200","201",toDecimal(202),(short)203,204,205L,206F,207D,208,toTimestamp(209
                           * millsPerDay),toBytes("210")
                       <li>2,"300","301",toDecimal(302),(short)303,304,305L,306F,307D,308,toTimestamp(309
                           * millsPerDay),toBytes("310")
                     </ul>
                    */
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(6, 200L, 500L);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 2L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    // Filter field "g" in [200, 500], get old file 1,2 (filter is forbidden) and
                    // new file 1
                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .between(6, 200F, 500F))
                                    .read()
                                    .dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 4L);

                    List<String> filesName =
                            files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
                    assertThat(filesName.size()).isGreaterThan(0);

                    List<DataFileMeta> fileMetaList =
                            splits.stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    assertThat(
                                    fileMetaList.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()))
                            .containsAll(filesName);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterPrimaryKeyFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // results of field "e" in [200, 500] in SCHEMA_FIELDS which is bigint
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(4, (short) 200, (short) 500);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 2L);
                    return splits.stream()
                            .flatMap(s -> s.dataFiles().stream())
                            .collect(Collectors.toList());
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // results of field "e" in [200, 500] in SCHEMA_FIELDS which is updated from
                    // bigint to int
                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .between(4, 200, 500))
                                    .read()
                                    .dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 3L);

                    List<String> filesName =
                            files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
                    assertThat(filesName.size()).isGreaterThan(0);

                    List<DataFileMeta> fileMetaList =
                            splits.stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    assertThat(
                                    fileMetaList.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()))
                            .containsAll(filesName);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Override
    protected List<String> getPrimaryKeyNames() {
        return SCHEMA_PRIMARY_KEYS;
    }

    protected abstract SimpleStats getTableValueStats(DataFileMeta fileMeta);
}
