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

import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests of {@link PrimaryKeyFileStoreTable} for schema evolution. */
public class PrimaryKeyFileDataTableTest extends FileDataFilterTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
    }

    @Test
    @Override
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
                                            "1|11|111|null|null|null",
                                            "1|13|113|null|null|null",
                                            "1|14|114|null|null|null",
                                            "1|21|121|1121|S011|S21",
                                            "1|22|122|1122|S012|S22"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    public void testReadFilterKeyField() throws Exception {
        writeAndCheckFileResult(
                schemas -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_0_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);
                    // scan filter with "kt" = 114 in schema0
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(builder.equal(4, 114L))
                                            .read()
                                            .dataSplits());
                    TableRead read = table.newRead();
                    assertThat(getResult(read, splits, SCHEMA_0_ROW_TO_STRING))
                            .hasSameElementsAs(Collections.singletonList("S004|1|14|S14|114|S114"));
                    return null;
                },
                (files, schemas) -> {
                    PredicateBuilder builder = new PredicateBuilder(new RowType(SCHEMA_1_FIELDS));
                    FileStoreTable table = createFileStoreTable(schemas);

                    // scan filter with "kt" = 114 in schema1
                    List<Split> splits =
                            toSplits(
                                    table.newSnapshotReader()
                                            .withFilter(builder.equal(2, 114L))
                                            .read()
                                            .dataSplits());
                    TableRead read1 = table.newRead();
                    assertThat(getResult(read1, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Collections.singletonList("1|14|114|null|null|null"));

                    // read filter with "kt" = 114 in schema1
                    splits = toSplits(table.newSnapshotReader().read().dataSplits());
                    TableRead read2 = table.newRead().withFilter(builder.equal(2, 114L));
                    assertThat(getResult(read2, splits, SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Collections.singletonList("1|14|114|null|null|null"));
                },
                getPrimaryKeyNames(),
                tableConfig,
                this::createFileStoreTable);
    }

    @Test
    @Override
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

                    /// TODO: changelog with key only supports to filter key
                    assertThat(getResult(read, splits, STREAMING_SCHEMA_0_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+S005|2|15|S15|115|S115",
                                            "+S006|2|16|S16|116|S116",
                                            "+S004|1|14|S14|114|S114"));
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
                    /// TODO: changelog with key only supports to filter on key
                    TableRead read1 = table.newRead().withFilter(builder.equal(1, 15));
                    assertThat(getResult(read1, splits, STREAMING_SCHEMA_1_ROW_TO_STRING))
                            .hasSameElementsAs(
                                    Arrays.asList(
                                            "+2|20|120|1120|S010|S20",
                                            "+1|21|121|1121|S011|S21",
                                            "+1|22|122|1122|S012|S22"));

                    // filter with "d" = 21 in schema1
                    /// TODO: changelog with key only supports to filter on key
                    TableRead read2 = table.newRead().withFilter(builder.equal(1, 21));
                    assertThat(getResult(read2, splits, STREAMING_SCHEMA_1_ROW_TO_STRING))
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
    public void testStreamingFilterKey() throws Exception {
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
                    // filter with "kt" = 116 in schema0
                    TableRead read = table.newRead().withFilter(builder.equal(4, 116L));

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

                    // filter with "kt" = 120 in schema1
                    TableRead read = table.newRead().withFilter(builder.equal(1, 120));
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
}
