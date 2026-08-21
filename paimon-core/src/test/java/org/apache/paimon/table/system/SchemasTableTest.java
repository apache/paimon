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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.JsonSerdeUtil.toFlatJson;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SchemasTable}. */
public class SchemasTableTest extends TableTestBase {

    private SchemasTable schemasTable;
    private SchemaManager schemaManager;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .build();
        catalog.createTable(identifier, schema, true);
        schemasTable = (SchemasTable) catalog.getTable(identifier("T$schemas"));

        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        schemaManager = new SchemaManager(fileIO, tablePath);
    }

    @Test
    public void testSchemasTable() throws Exception {
        List<InternalRow> expectRow = getExpectedResult();
        List<InternalRow> result = read(schemasTable);
        assertThat(result).containsExactlyElementsOf(expectRow);
    }

    @Test
    public void testReadSchemasWithInFilterContainingUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        Predicate predicate =
                builder.in(
                        schemasTable.rowType().getFieldNames().indexOf("schema_id"),
                        Arrays.asList(0L, 99L));

        ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(schemasTable.rowType());
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).containsExactlyElementsOf(getExpectedResult());
    }

    @Test
    public void testReadSchemasWithInAndRangeFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        List<Predicate> predicates =
                Arrays.asList(
                        PredicateBuilder.and(
                                builder.in(0, Arrays.asList(0L, 99L)),
                                builder.greaterOrEqual(0, 0L)),
                        PredicateBuilder.and(
                                builder.in(0, Collections.singletonList(0L)),
                                builder.greaterOrEqual(0, 1L)));
        for (int i = 0; i < predicates.size(); i++) {
            Predicate predicate = predicates.get(i);
            ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
            List<InternalRow> result = new ArrayList<>();
            InternalRowSerializer serializer = new InternalRowSerializer(schemasTable.rowType());
            readBuilder
                    .newRead()
                    .createReader(readBuilder.newScan().plan())
                    .forEachRemaining(row -> result.add(serializer.copy(row)));

            if (i == 0) {
                assertThat(result).extracting(row -> row.getLong(0)).containsExactly(0L);
            } else {
                assertThat(result).isEmpty();
            }
        }
    }

    @Test
    public void testReadSchemasWithEqualFilterOnUnknownId() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        Predicate predicate =
                builder.equal(schemasTable.rowType().getFieldNames().indexOf("schema_id"), 99L);

        ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(result::add);

        assertThat(result).isEmpty();
    }

    @Test
    public void testReadSchemasWithNestedAndFilter() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        Predicate predicate =
                PredicateBuilder.and(
                        builder.greaterOrEqual(0, 0L),
                        builder.equal(0, 99L),
                        builder.lessThan(0, 3L));

        ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
        List<InternalRow> result = new ArrayList<>();
        readBuilder
                .newRead()
                .createReader(readBuilder.newScan().plan())
                .forEachRemaining(result::add);

        assertThat(result).isEmpty();
    }

    @Test
    public void testReadSchemasWithExclusiveBoundOverflow() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        builder.greaterThan(0, Long.MAX_VALUE),
                        builder.lessThan(0, Long.MIN_VALUE))) {
            ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
            List<InternalRow> result = new ArrayList<>();
            readBuilder
                    .newRead()
                    .createReader(readBuilder.newScan().plan())
                    .forEachRemaining(result::add);

            assertThat(result).isEmpty();
        }
    }

    @Test
    public void testFilterBySchemaIdEqualAndGreaterOrEqual() throws Exception {
        catalog.alterTable(
                identifier("T"),
                Collections.singletonList(SchemaChange.addColumn("col2", DataTypes.INT())),
                false);
        schemasTable = (SchemasTable) catalog.getTable(identifier("T$schemas"));

        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        Predicate predicate =
                PredicateBuilder.and(builder.equal(0, 1L), builder.greaterOrEqual(0, 0L));
        ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        List<InternalRow> result = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(schemasTable.rowType());
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result).extracting(row -> row.getLong(0)).containsExactly(1L);
    }

    @Test
    public void testFilterBySchemaIdEqualAndLessOrEqual() throws Exception {
        catalog.alterTable(
                identifier("T"),
                Collections.singletonList(SchemaChange.addColumn("col2", DataTypes.INT())),
                false);
        schemasTable = (SchemasTable) catalog.getTable(identifier("T$schemas"));

        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        PredicateBuilder.and(builder.equal(0, 0L), builder.lessOrEqual(0, 1L)),
                        PredicateBuilder.and(builder.lessOrEqual(0, 1L), builder.equal(0, 0L)))) {
            ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
            RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan());
            List<InternalRow> result = new ArrayList<>();
            InternalRowSerializer serializer = new InternalRowSerializer(schemasTable.rowType());
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));

            assertThat(result).extracting(row -> row.getLong(0)).containsExactly(0L);
        }
    }

    @Test
    public void testFilterBySchemaIdWithEmptyRange() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(schemasTable.rowType());
        for (Predicate predicate :
                Arrays.asList(
                        PredicateBuilder.and(builder.equal(0, 0L), builder.greaterOrEqual(0, 1L)),
                        PredicateBuilder.and(
                                builder.greaterOrEqual(0, 1L), builder.equal(0, 0L)))) {
            ReadBuilder readBuilder = schemasTable.newReadBuilder().withFilter(predicate);
            RecordReader<InternalRow> reader =
                    readBuilder.newRead().createReader(readBuilder.newScan().plan());
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(result::add);

            assertThat(result).isEmpty();
        }
    }

    private List<InternalRow> getExpectedResult() {
        List<TableSchema> tableSchemas = schemaManager.listAll();

        List<InternalRow> expectedRow = new ArrayList<>();
        for (TableSchema schema : tableSchemas) {
            expectedRow.add(
                    GenericRow.of(
                            schema.id(),
                            BinaryString.fromString(toFlatJson(schema.fields())),
                            BinaryString.fromString(toFlatJson(schema.partitionKeys())),
                            BinaryString.fromString(toFlatJson(schema.primaryKeys())),
                            BinaryString.fromString(toFlatJson(schema.options())),
                            BinaryString.fromString(schema.comment()),
                            Timestamp.fromLocalDateTime(
                                    LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(schema.timeMillis()),
                                            ZoneId.systemDefault()))));
        }
        return expectedRow;
    }
}
