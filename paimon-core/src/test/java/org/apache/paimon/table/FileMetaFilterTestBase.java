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

import org.apache.paimon.format.FieldStats;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class of file meta for schema evolution in {@link FileStoreTable}. */
public abstract class FileMetaFilterTestBase extends SchemaEvolutionTableTestBase {

    @Test
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
                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .greaterOrEqual(1, 0))
                                    .read()
                                    .dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 12L);

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

                    for (DataFileMeta fileMeta : fileMetaList) {
                        FieldStats[] statsArray = getTableValueStats(fileMeta).fields(null);
                        assertThat(statsArray.length).isEqualTo(6);
                        if (filesName.contains(fileMeta.fileName())) {
                            assertThat(statsArray[0].minValue()).isNotNull();
                            assertThat(statsArray[0].maxValue()).isNotNull();
                            assertThat(statsArray[1].minValue()).isNotNull();
                            assertThat(statsArray[1].maxValue()).isNotNull();
                            assertThat(statsArray[2].minValue()).isNotNull();
                            assertThat(statsArray[2].maxValue()).isNotNull();

                            assertThat(statsArray[3].minValue()).isNull();
                            assertThat(statsArray[3].maxValue()).isNull();
                            assertThat(statsArray[4].minValue()).isNull();
                            assertThat(statsArray[4].maxValue()).isNull();
                            assertThat(statsArray[5].minValue()).isNull();
                            assertThat(statsArray[5].maxValue()).isNull();
                        } else {
                            assertThat(statsArray[0].minValue()).isNotNull();
                            assertThat(statsArray[0].maxValue()).isNotNull();
                            assertThat(statsArray[1].minValue()).isNotNull();
                            assertThat(statsArray[1].maxValue()).isNotNull();
                            assertThat(statsArray[2].minValue()).isNotNull();
                            assertThat(statsArray[2].maxValue()).isNotNull();
                            assertThat(statsArray[3].minValue()).isNotNull();
                            assertThat(statsArray[3].maxValue()).isNotNull();
                            assertThat(statsArray[4].minValue()).isNotNull();
                            assertThat(statsArray[4].maxValue()).isNotNull();
                            assertThat(statsArray[5].minValue()).isNotNull();
                            assertThat(statsArray[5].maxValue()).isNotNull();
                        }
                    }
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterExistFields() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // results of field "b" in [14, 19] in SCHEMA_0_FIELDS, "b" is renamed to "d" in
                    // SCHEMA_1_FIELDS
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(2, 14, 19);
                    List<DataFileMeta> files =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits()
                                    .stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    assertThat(files.size()).isGreaterThan(0);
                    checkFilterRowCount(files, 3L);
                    return files;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());
                    // results of field "d" in [14, 19] in SCHEMA_1_FIELDS
                    Predicate predicate = builder.between(1, 14, 19);
                    List<DataSplit> filterSplits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    List<DataFileMeta> filterFileMetas =
                            filterSplits.stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    checkFilterRowCount(filterFileMetas, 6L);

                    List<String> fileNameList =
                            filterFileMetas.stream()
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toList());
                    Set<String> fileNames =
                            filterFileMetas.stream()
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toSet());
                    assertThat(fileNameList.size()).isEqualTo(fileNames.size());

                    builder = new PredicateBuilder(table.schema().logicalRowType());
                    // get all meta files with filter
                    List<DataSplit> filterAllSplits =
                            table.newSnapshotReader()
                                    .withFilter(builder.greaterOrEqual(1, 0))
                                    .read()
                                    .dataSplits();
                    assertThat(
                                    filterAllSplits.stream()
                                            .flatMap(
                                                    s ->
                                                            s.dataFiles().stream()
                                                                    .map(DataFileMeta::fileName))
                                            .collect(Collectors.toList()))
                            .containsAll(
                                    files.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()));

                    // get all meta files without filter
                    List<DataSplit> allSplits = table.newSnapshotReader().read().dataSplits();
                    assertThat(filterAllSplits).isEqualTo(allSplits);

                    Set<String> filterFileNames = new HashSet<>();
                    for (DataSplit dataSplit : filterAllSplits) {
                        for (DataFileMeta dataFileMeta : dataSplit.dataFiles()) {
                            FieldStats[] fieldStats = getTableValueStats(dataFileMeta).fields(null);
                            int minValue = (Integer) fieldStats[1].minValue();
                            int maxValue = (Integer) fieldStats[1].maxValue();
                            if (minValue >= 14
                                    && minValue <= 19
                                    && maxValue >= 14
                                    && maxValue <= 19) {
                                filterFileNames.add(dataFileMeta.fileName());
                            }
                        }
                    }
                    assertThat(filterFileNames).isEqualTo(fileNames);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterNewFields() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<DataFileMeta> files =
                            table.newSnapshotReader().read().dataSplits().stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    assertThat(files.size()).isGreaterThan(0);
                    checkFilterRowCount(files, 6L);
                    return files;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());

                    // results of field "a" in (1120, -] in SCHEMA_1_FIELDS, "a" is not existed in
                    // SCHEMA_0_FIELDS
                    Predicate predicate = builder.greaterThan(3, 1120);
                    List<DataSplit> filterSplits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(filterSplits), 2L);

                    List<DataFileMeta> filterFileMetas =
                            filterSplits.stream()
                                    .flatMap(s -> s.dataFiles().stream())
                                    .collect(Collectors.toList());
                    List<String> fileNameList =
                            filterFileMetas.stream()
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toList());
                    Set<String> fileNames =
                            filterFileMetas.stream()
                                    .map(DataFileMeta::fileName)
                                    .collect(Collectors.toSet());
                    assertThat(fileNameList.size()).isEqualTo(fileNames.size());

                    List<String> filesName =
                            files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
                    assertThat(fileNameList).doesNotContainAnyElementsOf(filesName);

                    List<DataSplit> allSplits =
                            table.newSnapshotReader()
                                    .withFilter(builder.greaterOrEqual(1, 0))
                                    .read()
                                    .dataSplits();
                    checkFilterRowCount(toDataFileMetas(allSplits), 12L);

                    Set<String> filterFileNames = new HashSet<>();
                    for (DataSplit dataSplit : allSplits) {
                        for (DataFileMeta dataFileMeta : dataSplit.dataFiles()) {
                            FieldStats[] fieldStats = getTableValueStats(dataFileMeta).fields(null);
                            Integer minValue = (Integer) fieldStats[3].minValue();
                            Integer maxValue = (Integer) fieldStats[3].maxValue();
                            if (minValue != null
                                    && maxValue != null
                                    && minValue > 1120
                                    && maxValue > 1120) {
                                filterFileNames.add(dataFileMeta.fileName());
                            }
                        }
                    }
                    assertThat(filterFileNames).isEqualTo(fileNames);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterPartition() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    checkFilterRowCount(table, 1, 1, 3L);
                    checkFilterRowCount(table, 1, 2, 3L);
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    checkFilterRowCount(table, 0, 1, 7L);
                    checkFilterRowCount(table, 0, 2, 5L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterPrimaryKey() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());
                    Predicate predicate = builder.between(4, 115L, 120L);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 2L);
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    PredicateBuilder builder =
                            new PredicateBuilder(table.schema().logicalRowType());
                    Predicate predicate = builder.between(2, 115L, 120L);
                    List<DataSplit> splits =
                            table.newSnapshotReader().withFilter(predicate).read().dataSplits();
                    checkFilterRowCount(toDataFileMetas(splits), 6L);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    protected abstract BinaryTableStats getTableValueStats(DataFileMeta fileMeta);

    protected static void checkFilterRowCount(
            FileStoreTable table, int index, int value, long expectedRowCount) {
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        List<DataSplit> splits =
                table.newSnapshotReader()
                        .withFilter(builder.equal(index, value))
                        .read()
                        .dataSplits();
        checkFilterRowCount(toDataFileMetas(splits), expectedRowCount);
    }
}
