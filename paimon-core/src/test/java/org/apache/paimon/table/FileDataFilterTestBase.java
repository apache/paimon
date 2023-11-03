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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class of file data for schema evolution in {@link FileStoreTable}. */
public abstract class FileDataFilterTestBase extends SchemaEvolutionTableTestBase {

    protected static final int[] PROJECTION = new int[] {3, 2, 1};

    protected static final Function<InternalRow, String> SCHEMA_0_ROW_TO_STRING =
            rowData ->
                    getNullOrString(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2)
                            + "|"
                            + getNullOrString(rowData, 3)
                            + "|"
                            + getNullOrLong(rowData, 4)
                            + "|"
                            + getNullOrString(rowData, 5);

    protected static final Function<InternalRow, String> STREAMING_SCHEMA_0_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + getNullOrString(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2)
                            + "|"
                            + getNullOrString(rowData, 3)
                            + "|"
                            + getNullOrLong(rowData, 4)
                            + "|"
                            + getNullOrString(rowData, 5);

    protected static final Function<InternalRow, String> SCHEMA_0_PROJECT_ROW_TO_STRING =
            rowData ->
                    getNullOrString(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2);

    protected static final Function<InternalRow, String> STREAMING_SCHEMA_0_PROJECT_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + getNullOrString(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2);

    protected static final Function<InternalRow, String> SCHEMA_1_ROW_TO_STRING =
            rowData ->
                    getNullOrInt(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrLong(rowData, 2)
                            + "|"
                            + getNullOrInt(rowData, 3)
                            + "|"
                            + getNullOrString(rowData, 4)
                            + "|"
                            + getNullOrString(rowData, 5);

    protected static final Function<InternalRow, String> STREAMING_SCHEMA_1_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + getNullOrInt(rowData, 0)
                            + "|"
                            + getNullOrInt(rowData, 1)
                            + "|"
                            + getNullOrLong(rowData, 2)
                            + "|"
                            + getNullOrInt(rowData, 3)
                            + "|"
                            + getNullOrString(rowData, 4)
                            + "|"
                            + getNullOrString(rowData, 5);

    protected static final Function<InternalRow, String> SCHEMA_1_PROJECT_ROW_TO_STRING =
            rowData ->
                    getNullOrInt(rowData, 0)
                            + "|"
                            + getNullOrLong(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2);

    protected static final Function<InternalRow, String> STREAMING_SCHEMA_1_PROJECT_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + getNullOrInt(rowData, 0)
                            + "|"
                            + getNullOrLong(rowData, 1)
                            + "|"
                            + getNullOrInt(rowData, 2);

    private static String getNullOrInt(InternalRow rowData, int index) {
        return rowData.isNullAt(index) ? "null" : String.valueOf(rowData.getInt(index));
    }

    private static String getNullOrLong(InternalRow rowData, int index) {
        return rowData.isNullAt(index) ? "null" : String.valueOf(rowData.getLong(index));
    }

    private static String getNullOrString(InternalRow rowData, int index) {
        return rowData.isNullAt(index) ? "null" : rowData.getString(index).toString();
    }

    @Test
    public void testReadFilterExistField() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_0_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    // filter with "b" = 15 in schema0
                    TableRead read = table.newRead().withFilter(builder.equal(2, 15));

                    assertThat(getResult(read, splits, SCHEMA_0_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "S005|2|15|S15|115|S115", "S006|2|16|S16|116|S116"));
                    return null;
                },
                (files, schemas) -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_1_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());

                    // filter with "d" = 15 in schema1 which should be mapped to "b" = 15 in schema0
                    TableRead read1 = table.newRead().withFilter(builder.equal(1, 15));
                    assertThat(getResult(read1, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "2|15|115|null|null|null", "2|16|116|null|null|null"));

                    // filter with "d" = 21 in schema1
                    TableRead read2 = table.newRead().withFilter(builder.equal(1, 21));
                    assertThat(getResult(read2, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "1|21|121|1121|S011|S21", "1|22|122|1122|S012|S22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testReadFilterNonExistField() throws Exception {
        writeAndCheckFileResult(
                schemas -> null,
                (files, schemas) -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_1_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());

                    // filter with "a" = 1122 in schema1 which is not exist in schema0
                    TableRead read1 = table.newRead().withFilter(builder.equal(3, 1122));
                    assertThat(getResult(read1, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "2|12|112|null|null|null",
                                            "2|15|115|null|null|null",
                                            "2|16|116|null|null|null",
                                            "1|11|111|null|null|null",
                                            "1|13|113|null|null|null",
                                            "1|14|114|null|null|null",
                                            "1|21|121|1121|S011|S21",
                                            "1|22|122|1122|S012|S22"));

                    // filter with "a" = 1122 in scan and read
                    splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(builder.equal(3, 1122))
                                            .read()
                                            .dataSplits());
                    TableRead read2 = table.newRead().withFilter(builder.equal(3, 1122));
                    assertThat(getResult(read2, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "1|21|121|1121|S011|S21", "1|22|122|1122|S012|S22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testReadFilterMultipleFields() throws Exception {
        writeAndCheckFileResult(
                schemas -> null,
                (files, schemas) -> {
                    List<Predicate> predicateList =
                            Arrays.asList(
                                    new LeafPredicate(
                                            Equal.INSTANCE,
                                            DataTypes.INT(),
                                            1,
                                            "d",
                                            Collections.singletonList(21)),
                                    new LeafPredicate(
                                            IsNull.INSTANCE,
                                            DataTypes.STRING(),
                                            4,
                                            "f",
                                            Collections.emptyList()));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());

                    // filter with "d" = 21 or "f" is null in schema1 that "f" is not exist in
                    // schema0, read all data
                    TableRead read1 =
                            table.newRead().withFilter(PredicateBuilder.or(predicateList));
                    assertThat(getResult(read1, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "2|12|112|null|null|null",
                                            "2|15|115|null|null|null",
                                            "2|16|116|null|null|null",
                                            "1|11|111|null|null|null",
                                            "1|13|113|null|null|null",
                                            "1|14|114|null|null|null",
                                            "1|21|121|1121|S011|S21",
                                            "1|22|122|1122|S012|S22"));

                    splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    // filter with "d" = 21 and "f" is null, read snapshot which contains "d" = 21
                    TableRead read2 =
                            table.newRead().withFilter(PredicateBuilder.and(predicateList));
                    assertThat(getResult(read2, splits, SCHEMA_1_ROW_TO_STRING)).isEmpty();
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testBatchProjection() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    // project "c", "b", "pt" in schema0
                    TableRead read = table.newRead().withProjection(PROJECTION);

                    assertThat(getResult(read, splits, SCHEMA_0_PROJECT_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "S12|12|2",
                                            "S15|15|2",
                                            "S16|16|2",
                                            "S11|11|1",
                                            "S13|13|1",
                                            "S14|14|1"));
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());

                    // project "a", "kt", "d" in schema1
                    TableRead read = table.newRead().withProjection(PROJECTION);
                    assertThat(getResult(read, splits, SCHEMA_1_PROJECT_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "null|112|12",
                                            "1120|120|20",
                                            "null|115|15",
                                            "null|116|16",
                                            "1118|118|18",
                                            "null|111|11",
                                            "null|113|13",
                                            "null|114|14",
                                            "1121|121|21",
                                            "1122|122|22",
                                            "1117|117|17",
                                            "1119|119|19"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testStreamingReadWrite() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());
                    TableRead read = table.newRead();

                    assertThat(getResult(read, splits, STREAMING_SCHEMA_0_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+S005|2|15|S15|115|S115",
                                            "+S006|2|16|S16|116|S116",
                                            "+S004|1|14|S14|114|S114"));
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());

                    TableRead read = table.newRead();
                    assertThat(getResult(read, splits, STREAMING_SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+2|20|120|1120|S010|S20",
                                            "+1|21|121|1121|S011|S21",
                                            "+1|22|122|1122|S012|S22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testStreamingProjection() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());
                    // project "c", "b", "pt" in schema0
                    TableRead read = table.newRead().withProjection(PROJECTION);

                    assertThat(getResult(read, splits, STREAMING_SCHEMA_0_PROJECT_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList("+S15|15|2", "+S16|16|2", "+S14|14|1"));
                    return null;
                },
                (files, schemas) -> {
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());

                    // project "a", "kt", "d" in schema1
                    TableRead read = table.newRead().withProjection(PROJECTION);
                    assertThat(getResult(read, splits, STREAMING_SCHEMA_1_PROJECT_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList("+1120|120|20", "+1121|121|21", "+1122|122|22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testStreamingFilter() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_0_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());
                    // filter with "b" = 15 in schema0
                    TableRead read = table.newRead().withFilter(builder.equal(2, 15));

                    assertThat(getResult(read, splits, STREAMING_SCHEMA_0_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+S005|2|15|S15|115|S115", "+S006|2|16|S16|116|S116"));
                    return null;
                },
                (files, schemas) -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_1_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withMode(ScanMode.DELTA)
                                            .read()
                                            .dataSplits());

                    // filter with "d" = 15 in schema1 which should be mapped to "b" = 15 in schema0
                    TableRead read1 = table.newRead().withFilter(builder.equal(1, 15));
                    assertThat(getResult(read1, splits, STREAMING_SCHEMA_1_ROW_TO_STRING))
                            .isEmpty();

                    // filter with "d" = 21 in schema1
                    TableRead read2 = table.newRead().withFilter(builder.equal(1, 21));
                    assertThat(getResult(read2, splits, STREAMING_SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+1|21|121|1121|S011|S21", "+1|22|122|1122|S012|S22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }
}
