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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
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

                    /*
                     Filter field "g" in [200, 500] in SCHEMA_FIELDS which is updated from bigint
                     to float and will get another file with one data as followed:

                     <ul>
                       <li>2,"400","401",402D,403,toDecimal(404),405F,406D,toDecimal(407),408,409,toBytes("410")
                     </ul>

                     <p>Then we can check the results of the two result files.
                    */
                    List<DataSplit> splits =
                            table.newSnapshotReader()
                                    .withFilter(
                                            new PredicateBuilder(table.schema().logicalRowType())
                                                    .between(6, 200F, 500F))
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

                    validateValuesWithNewSchema(
                            schemas, table.schema().id(), filesName, fileMetaList);
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

                    // Compare all columns with table column type
                    validateValuesWithNewSchema(
                            schemas, table.schema().id(), filesName, fileMetaList);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    protected void validateValuesWithNewSchema(
            Map<Long, TableSchema> tableSchemas,
            long schemaId,
            List<String> filesName,
            List<DataFileMeta> fileMetaList) {
        Function<Long, List<DataField>> schemaFields = id -> tableSchemas.get(id).fields();
        SimpleStatsEvolutions converters = new SimpleStatsEvolutions(schemaFields, schemaId);
        for (DataFileMeta fileMeta : fileMetaList) {
            SimpleStats stats = getTableValueStats(fileMeta);
            SimpleStatsEvolution.Result result =
                    converters.getOrCreate(fileMeta.schemaId()).evolution(stats, null, null);
            InternalRow min = result.minValues();
            InternalRow max = result.maxValues();
            assertThat(stats.minValues().getFieldCount()).isEqualTo(12);
            if (filesName.contains(fileMeta.fileName())) {
                checkTwoValues(min, max);
            } else {
                checkOneValue(min, max);
            }
        }
    }

    /**
     * Check file data with one data.
     *
     * <ul>
     *   <li>data:
     *       2,"400","401",402D,403,toDecimal(404),405F,406D,toDecimal(407),408,409,toBytes("410")
     *   <li>types: a->int, b->varchar[10], c->varchar[10], d->double, e->int, f->decimal,g->float,
     *       h->double, i->decimal, j->date, k->date, l->varbinary
     * </ul>
     */
    private void checkOneValue(InternalRow min, InternalRow max) {
        assertThat(min.getInt(0)).isEqualTo(max.getInt(0)).isEqualTo(2);
        assertThat(min.getString(1))
                .isEqualTo(max.getString(1))
                .isEqualTo(BinaryString.fromString("400"));
        assertThat(min.getString(2))
                .isEqualTo(max.getString(2))
                .isEqualTo(BinaryString.fromString("401"));
        assertThat(min.getDouble(3)).isEqualTo(max.getDouble(3)).isEqualTo(402D);
        assertThat(min.getInt(4)).isEqualTo(max.getInt(4)).isEqualTo(403);
        assertThat(min.getDecimal(5, 10, 2).toBigDecimal().intValue())
                .isEqualTo(max.getDecimal(5, 10, 2).toBigDecimal().intValue())
                .isEqualTo(404);
        assertThat(min.getFloat(6)).isEqualTo(max.getFloat(6)).isEqualTo(405F);
        assertThat(min.getDouble(7)).isEqualTo(max.getDouble(7)).isEqualTo(406D);
        assertThat(min.getDecimal(8, 10, 2).toBigDecimal().doubleValue())
                .isEqualTo(max.getDecimal(8, 10, 2).toBigDecimal().doubleValue())
                .isEqualTo(407D);
        assertThat(min.getInt(9)).isEqualTo(max.getInt(9)).isEqualTo(408);
        assertThat(min.getInt(10)).isEqualTo(max.getInt(10)).isEqualTo(409);
        assertThat(min.isNullAt(11)).isEqualTo(max.isNullAt(11)).isTrue();
    }

    /**
     * Check file with new types and data.
     *
     * <ul>
     *   <li>data1: 2,"200","201",toDecimal(202),(short)203,204,205L,206F,207D,208,toTimestamp(209 *
     *       millsPerDay),toBytes("210")
     *   <li>data2: 2,"300","301",toDecimal(302),(short)303,304,305L,306F,307D,308,toTimestamp(309 *
     *       millsPerDay),toBytes("310")
     *   <li>old types: a->int, b->char[10], c->varchar[10], d->decimal, e->smallint, f->int,
     *       g->bigint, h->float, i->double, j->date, k->timestamp, l->binary
     *   <li>new types: a->int, b->varchar[10], c->varchar[10], d->double, e->int,
     *       f->decimal,g->float, h->double, i->decimal, j->date, k->date, l->varbinary
     * </ul>
     */
    private void checkTwoValues(InternalRow min, InternalRow max) {
        assertThat(min.getInt(0)).isEqualTo(2);
        assertThat(max.getInt(0)).isEqualTo(2);

        // parquet does not support padding
        assertThat(min.getString(1).toString()).startsWith("200");
        assertThat(max.getString(1).toString()).startsWith("300");

        assertThat(min.getString(2)).isEqualTo(BinaryString.fromString("201"));
        assertThat(max.getString(2)).isEqualTo(BinaryString.fromString("301"));

        assertThat(min.getDouble(3)).isEqualTo(202D);
        assertThat(max.getDouble(3)).isEqualTo(302D);

        assertThat(min.getInt(4)).isEqualTo(203);
        assertThat(max.getInt(4)).isEqualTo(303);

        assertThat(min.getDecimal(5, 10, 2).toBigDecimal().intValue()).isEqualTo(204);
        assertThat(max.getDecimal(5, 10, 2).toBigDecimal().intValue()).isEqualTo(304);

        assertThat(min.getFloat(6)).isEqualTo(205F);
        assertThat(max.getFloat(6)).isEqualTo(305F);

        assertThat(min.getDouble(7)).isEqualTo(206D);
        assertThat(max.getDouble(7)).isEqualTo(306D);

        assertThat(min.getDecimal(8, 10, 2).toBigDecimal().doubleValue()).isEqualTo(207D);
        assertThat(max.getDecimal(8, 10, 2).toBigDecimal().doubleValue()).isEqualTo(307D);

        assertThat(min.getInt(9)).isEqualTo(208);
        assertThat(max.getInt(9)).isEqualTo(308);

        assertThat(min.getInt(10)).isEqualTo(209);
        assertThat(max.getInt(10)).isEqualTo(309);

        // Min and max value of binary type is null
        assertThat(min.isNullAt(11)).isTrue();
        assertThat(max.isNullAt(11)).isTrue();
    }

    @Override
    protected List<String> getPrimaryKeyNames() {
        return SCHEMA_PRIMARY_KEYS;
    }

    protected abstract SimpleStats getTableValueStats(DataFileMeta fileMeta);
}
