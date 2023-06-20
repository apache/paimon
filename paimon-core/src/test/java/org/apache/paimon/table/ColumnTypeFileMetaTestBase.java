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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.BinaryTableStats;
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
                            .flatMap(s -> s.files().stream())
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
                                    .flatMap(s -> s.files().stream())
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
            FieldStats[] statsArray = getTableValueStats(fileMeta).fields(null);
            assertThat(statsArray.length).isEqualTo(12);
            for (int i = 0; i < 11; i++) {
                assertThat(statsArray[i].minValue()).isNotNull();
                assertThat(statsArray[i].maxValue()).isNotNull();
            }
            // Min and max value of binary type is null
            assertThat(statsArray[11].minValue()).isNull();
            assertThat(statsArray[11].maxValue()).isNull();
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
                            .flatMap(s -> s.files().stream())
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
                                    .flatMap(s -> s.files().stream())
                                    .collect(Collectors.toList());
                    assertThat(
                                    fileMetaList.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()))
                            .containsAll(filesName);

                    validateValuesWithNewSchema(filesName, fileMetaList);
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
                            .flatMap(s -> s.files().stream())
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
                                    .flatMap(s -> s.files().stream())
                                    .collect(Collectors.toList());
                    assertThat(
                                    fileMetaList.stream()
                                            .map(DataFileMeta::fileName)
                                            .collect(Collectors.toList()))
                            .containsAll(filesName);

                    // Compare all columns with table column type
                    validateValuesWithNewSchema(filesName, fileMetaList);
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    protected void validateValuesWithNewSchema(
            List<String> filesName, List<DataFileMeta> fileMetaList) {
        for (DataFileMeta fileMeta : fileMetaList) {
            FieldStats[] statsArray = getTableValueStats(fileMeta).fields(null);
            assertThat(statsArray.length).isEqualTo(12);
            if (filesName.contains(fileMeta.fileName())) {
                checkTwoValues(statsArray);
            } else {
                checkOneValue(statsArray);
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
     *
     * @param statsArray the field stats
     */
    private void checkOneValue(FieldStats[] statsArray) {
        assertThat(statsArray[0].minValue()).isEqualTo((statsArray[0].maxValue())).isEqualTo(2);
        assertThat(statsArray[1].minValue())
                .isEqualTo(statsArray[1].maxValue())
                .isEqualTo(BinaryString.fromString("400"));
        assertThat(statsArray[2].minValue())
                .isEqualTo(statsArray[2].maxValue())
                .isEqualTo(BinaryString.fromString("401"));
        assertThat((Double) statsArray[3].minValue())
                .isEqualTo((Double) statsArray[3].maxValue())
                .isEqualTo(402D);
        assertThat((Integer) statsArray[4].minValue())
                .isEqualTo(statsArray[4].maxValue())
                .isEqualTo(403);
        assertThat(((Decimal) statsArray[5].minValue()).toBigDecimal().intValue())
                .isEqualTo(((Decimal) statsArray[5].maxValue()).toBigDecimal().intValue())
                .isEqualTo(404);
        assertThat((Float) statsArray[6].minValue())
                .isEqualTo(statsArray[6].maxValue())
                .isEqualTo(405F);
        assertThat((Double) statsArray[7].minValue())
                .isEqualTo(statsArray[7].maxValue())
                .isEqualTo(406D);
        assertThat(((Decimal) statsArray[8].minValue()).toBigDecimal().doubleValue())
                .isEqualTo(((Decimal) statsArray[8].maxValue()).toBigDecimal().doubleValue())
                .isEqualTo(407D);
        assertThat(statsArray[9].minValue()).isEqualTo(statsArray[9].maxValue()).isEqualTo(408);
        assertThat(statsArray[10].minValue()).isEqualTo(statsArray[10].maxValue()).isEqualTo(409);

        // Min and max value of binary type is null
        assertThat(statsArray[11].minValue()).isNull();
        assertThat(statsArray[11].maxValue()).isNull();
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
    private void checkTwoValues(FieldStats[] statsArray) {
        assertThat(statsArray[0].minValue()).isEqualTo(2);
        assertThat(statsArray[0].maxValue()).isEqualTo(2);

        assertThat(statsArray[1].minValue()).isEqualTo(BinaryString.fromString("200       "));
        assertThat(statsArray[1].maxValue()).isEqualTo(BinaryString.fromString("300       "));

        assertThat(statsArray[2].minValue()).isEqualTo(BinaryString.fromString("201"));
        assertThat(statsArray[2].maxValue()).isEqualTo(BinaryString.fromString("301"));

        assertThat((Double) statsArray[3].minValue()).isEqualTo(202D);
        assertThat((Double) statsArray[3].maxValue()).isEqualTo(302D);

        assertThat((Integer) statsArray[4].minValue()).isEqualTo(203);
        assertThat((Integer) statsArray[4].maxValue()).isEqualTo(303);

        assertThat(((Decimal) statsArray[5].minValue()).toBigDecimal().intValue()).isEqualTo(204);
        assertThat(((Decimal) statsArray[5].maxValue()).toBigDecimal().intValue()).isEqualTo(304);

        assertThat((Float) statsArray[6].minValue()).isEqualTo(205F);
        assertThat((Float) statsArray[6].maxValue()).isEqualTo(305F);

        assertThat((Double) statsArray[7].minValue()).isEqualTo(206D);
        assertThat((Double) statsArray[7].maxValue()).isEqualTo(306D);

        assertThat(((Decimal) statsArray[8].minValue()).toBigDecimal().doubleValue())
                .isEqualTo(207D);
        assertThat(((Decimal) statsArray[8].maxValue()).toBigDecimal().doubleValue())
                .isEqualTo(307D);

        assertThat(statsArray[9].minValue()).isEqualTo(208);
        assertThat(statsArray[9].maxValue()).isEqualTo(308);
        assertThat(statsArray[10].minValue()).isEqualTo(209);
        assertThat(statsArray[10].maxValue()).isEqualTo(309);

        // Min and max value of binary type is null
        assertThat(statsArray[11].minValue()).isNull();
        assertThat(statsArray[11].maxValue()).isNull();
    }

    @Override
    protected List<String> getPrimaryKeyNames() {
        return SCHEMA_PRIMARY_KEYS;
    }

    protected abstract BinaryTableStats getTableValueStats(DataFileMeta fileMeta);
}
