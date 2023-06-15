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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InternalRowUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class of file data for column type evolution. */
public abstract class ColumnTypeFileDataTestBase extends SchemaEvolutionTableTestBase {
    protected static List<InternalRow.FieldGetter> getFieldGetterList(FileStoreTable table) {
        return Arrays.asList(
                InternalRowUtils.createFieldGetters(
                        table.schema().fields().stream()
                                .map(DataField::type)
                                .collect(Collectors.toList())));
    }

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
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    // scan all data with original column type
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.00|203|204|205|206.0|207.0|208|1970-07-29T00:00|210",
                                    "2|300|301|302.00|303|304|305|306.0|307.0|308|1970-11-06T00:00|310",
                                    "1|100|101|102.00|103|104|105|106.0|107.0|108|1970-04-20T00:00|110");
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.0|203|204.00|205.0|206.0|207.00|208|209|210",
                                    "2|300|301|302.0|303|304.00|305.0|306.0|307.00|308|309|310",
                                    "2|400|401|402.0|403|404.00|405.0|406.0|407.00|408|409|410",
                                    "1|100|101|102.0|103|104.00|105.0|106.0|107.00|108|109|110",
                                    "1|500|501|502.0|503|504.00|505.0|506.0|507.00|508|509|510",
                                    "1|600|601|602.0|603|604.00|605.0|606.0|607.00|608|609|610");
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterNormalFields() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    /**
                     * Filter field "g" in [200, 500] in SCHEMA_FIELDS which is bigint and will get
                     * one file with two data as followed:
                     *
                     * <ul>
                     *   <li>2,"200","201",toDecimal(202),(short)203,204,205L,206F,207D,208,toTimestamp(209
                     *       * millsPerDay),toBytes("210")
                     *   <li>2,"300","301",toDecimal(302),(short)303,304,305L,306F,307D,308,toTimestamp(309
                     *       * millsPerDay),toBytes("310")
                     * </ul>
                     */
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(6, 200L, 500L);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(predicate)
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.00|203|204|205|206.0|207.0|208|1970-07-29T00:00|210",
                                    "2|300|301|302.00|303|304|305|306.0|307.0|308|1970-11-06T00:00|310");
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    /**
                     * Filter field "g" in [200, 500] in SCHEMA_FIELDS which is updated from bigint
                     * to float and will get another file with one data as followed:
                     *
                     * <ul>
                     *   <li>2,"400","401",402D,403,toDecimal(404),405F,406D,toDecimal(407),408,409,toBytes("410")
                     * </ul>
                     */
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(
                                                    new PredicateBuilder(
                                                                    table.schema().logicalRowType())
                                                            .between(6, 200F, 500F))
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.0|203|204.00|205.0|206.0|207.00|208|209|210",
                                    "2|300|301|302.0|303|304.00|305.0|306.0|307.00|308|309|310",
                                    "2|400|401|402.0|403|404.00|405.0|406.0|407.00|408|409|410");
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testTableSplitFilterPrimaryKeys() throws Exception {
        writeAndCheckFileResultForColumnType(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    // results of field "e" in [200, 500] in SCHEMA_FIELDS which is bigint
                    Predicate predicate =
                            new PredicateBuilder(table.schema().logicalRowType())
                                    .between(4, (short) 200, (short) 500);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(predicate)
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.00|203|204|205|206.0|207.0|208|1970-07-29T00:00|210",
                                    "2|300|301|302.00|303|304|305|306.0|307.0|308|1970-11-06T00:00|310");
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);

                    // results of field "e" in [200, 500] in SCHEMA_FIELDS which is updated from
                    // bigint to int
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(
                                                    new PredicateBuilder(
                                                                    table.schema().logicalRowType())
                                                            .between(4, 200, 500))
                                            .read()
                                            .dataSplits());
                    List<InternalRow.FieldGetter> fieldGetterList = getFieldGetterList(table);
                    assertThat(getResult(table.newRead(), splits, fieldGetterList))
                            .containsExactlyInAnyOrder(
                                    "2|200|201|202.0|203|204.00|205.0|206.0|207.00|208|209|210",
                                    "2|300|301|302.0|303|304.00|305.0|306.0|307.00|308|309|310",
                                    "2|400|401|402.0|403|404.00|405.0|406.0|407.00|408|409|410");
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Override
    protected List<String> getPrimaryKeyNames() {
        return SCHEMA_PRIMARY_KEYS;
    }
}
