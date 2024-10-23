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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.paimon.table.SpecialFields.KEY_FIELD_PREFIX;
import static org.apache.paimon.table.SpecialFields.SYSTEM_FIELD_NAMES;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for schema evolution. */
public class SchemaEvolutionTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private Identifier identifier;
    private SchemaManager schemaManager;
    private String commitUser;

    @BeforeEach
    public void beforeEach() {
        tablePath = new Path(tempDir.toUri());
        identifier = SchemaManager.identifierFromPath(tablePath.toString(), true);
        schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testDefaultValue() throws Exception {
        {
            Map<String, String> option = new HashMap<>();
            option.put(
                    String.format(
                            "%s.%s.%s",
                            CoreOptions.FIELDS_PREFIX, "a", CoreOptions.DEFAULT_VALUE_SUFFIX),
                    "1");
            Schema schema =
                    new Schema(
                            RowType.of(
                                            new DataType[] {
                                                DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
                                                DataTypes.BIGINT()
                                            },
                                            new String[] {"a", "b"})
                                    .getFields(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            option,
                            "");

            assertThatThrownBy(() -> schemaManager.createTable(schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "The column %s with datatype %s is currently not supported for default value.",
                            "a", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()).asSQLString());
        }

        {
            Map<String, String> option = new HashMap<>();
            option.put(
                    String.format(
                            "%s.%s.%s",
                            CoreOptions.FIELDS_PREFIX, "a", CoreOptions.DEFAULT_VALUE_SUFFIX),
                    "abcxxxx");
            Schema schema =
                    new Schema(
                            RowType.of(
                                            new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT()},
                                            new String[] {"a", "b"})
                                    .getFields(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            option,
                            "");
            assertThatThrownBy(() -> schemaManager.createTable(schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "The default value %s of the column a can not be cast to datatype: %s",
                            "abcxxxx", DataTypes.BIGINT().asSQLString());
        }

        {
            Schema schema =
                    new Schema(
                            RowType.of(
                                            new DataType[] {
                                                DataTypes.BIGINT(),
                                                DataTypes.BIGINT(),
                                                DataTypes.BIGINT()
                                            },
                                            new String[] {"a", "b", "c"})
                                    .getFields(),
                            Lists.newArrayList("c"),
                            Lists.newArrayList("a", "c"),
                            new HashMap<>(),
                            "");

            schemaManager.createTable(schema);

            assertThatThrownBy(
                            () ->
                                    schemaManager.commitChanges(
                                            Collections.singletonList(
                                                    SchemaChange.setOption(
                                                            String.format(
                                                                    "%s.%s.%s",
                                                                    CoreOptions.FIELDS_PREFIX,
                                                                    "b",
                                                                    CoreOptions
                                                                            .DEFAULT_VALUE_SUFFIX),
                                                            "abcxxxx"))))
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "The default value %s of the column b can not be cast to datatype: %s",
                            "abcxxxx", DataTypes.BIGINT().asSQLString());
            assertThatThrownBy(
                            () ->
                                    schemaManager.commitChanges(
                                            Collections.singletonList(
                                                    SchemaChange.setOption(
                                                            String.format(
                                                                    "%s.%s.%s",
                                                                    CoreOptions.FIELDS_PREFIX,
                                                                    "a",
                                                                    CoreOptions
                                                                            .DEFAULT_VALUE_SUFFIX),
                                                            "abc"))))
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Primary key a should not be assign default column.");

            assertThatThrownBy(
                            () ->
                                    schemaManager.commitChanges(
                                            Collections.singletonList(
                                                    SchemaChange.setOption(
                                                            String.format(
                                                                    "%s.%s.%s",
                                                                    CoreOptions.FIELDS_PREFIX,
                                                                    "c",
                                                                    CoreOptions
                                                                            .DEFAULT_VALUE_SUFFIX),
                                                            "abc"))))
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Partition key c should not be assign default column.");
        }
    }

    @Test
    public void testAddField() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        StreamTableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(1, 1L));
        write.write(GenericRow.of(2, 2L));
        TableCommitImpl commit = table.newCommit(commitUser);
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.addColumn("f3", DataTypes.BIGINT())));
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        write = table.newWrite(commitUser);
        write.write(GenericRow.of(3, 3L, 3L));
        write.write(GenericRow.of(4, 4L, 4L));
        commit = table.newCommit(commitUser);
        commit.commit(1, write.prepareCommit(true, 1));
        write.close();
        commit.close();

        // read all
        List<String> rows = readRecords(table, null);
        assertThat(rows)
                .containsExactlyInAnyOrder("1, 1, NULL", "2, 2, NULL", "3, 3, 3", "4, 4, 4");

        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        // read where f0 = 1 (filter on old field)
        rows = readRecords(table, builder.equal(0, 1));
        assertThat(rows).containsExactlyInAnyOrder("1, 1, NULL", "2, 2, NULL");

        // read where f3 is null (filter on new field)
        rows = readRecords(table, builder.isNull(2));
        assertThat(rows).containsExactlyInAnyOrder("1, 1, NULL", "2, 2, NULL");

        // read where f3 = 3 (filter on new field)
        rows = readRecords(table, builder.equal(2, 3L));
        assertThat(rows).containsExactlyInAnyOrder("3, 3, 3", "4, 4, 4");

        // test add not null field
        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.addColumn(
                                                        "f4",
                                                        DataTypes.INT().copy(false),
                                                        null,
                                                        null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "Column %s cannot specify NOT NULL in the %s table.",
                                "f4", identifier.getFullName()));
    }

    @Test
    public void testAddDuplicateField() throws Exception {
        final String columnName = "f3";
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);
        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.addColumn(columnName, DataTypes.BIGINT())));
        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.addColumn(
                                                        columnName, DataTypes.FLOAT()))))
                .isInstanceOf(Catalog.ColumnAlreadyExistException.class)
                .hasMessage(
                        "Column %s already exists in the %s table.",
                        columnName, identifier.getFullName());
    }

    @Test
    public void testUpdateFieldType() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT(), "f0 field")
                        .column("f1", DataTypes.BIGINT())
                        .build();
        schemaManager.createTable(schema);

        TableSchema tableSchema =
                schemaManager.commitChanges(
                        Collections.singletonList(
                                SchemaChange.updateColumnType("f0", DataTypes.BIGINT())));
        assertThat(tableSchema.fields().get(0).type()).isEqualTo(DataTypes.BIGINT());
        assertThat(tableSchema.fields().get(0).description()).isEqualTo("f0 field");

        // bigint to string
        tableSchema =
                schemaManager.commitChanges(
                        Collections.singletonList(
                                SchemaChange.updateColumnType("f0", DataTypes.STRING())));
        assertThat(tableSchema.fields().get(0).type()).isEqualTo(DataTypes.STRING());
    }

    @Test
    public void testRenameField() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f0", "f1");

        // Rename "f0" to "f01", "f1" to "f0", "f01" to "f1"
        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.renameColumn("f0", "f01")));
        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.renameColumn("f1", "f0")));
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f01", "f0");
        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.renameColumn("f01", "f1")));
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f1", "f0");

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.renameColumn("f0", "f1"))))
                .isInstanceOf(Catalog.ColumnAlreadyExistException.class)
                .hasMessage(
                        String.format(
                                "Column %s already exists in the %s table.",
                                "f0", identifier.getFullName()));
    }

    @Test
    public void testDropField() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(
                                        DataTypes.INT(),
                                        DataTypes.BIGINT(),
                                        DataTypes.INT(),
                                        DataTypes.BIGINT())
                                .getFields(),
                        Collections.singletonList("f0"),
                        Arrays.asList("f0", "f2"),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);
        assertThat(schemaManager.latest().get().fieldNames())
                .containsExactly("f0", "f1", "f2", "f3");

        schemaManager.commitChanges(Collections.singletonList(SchemaChange.dropColumn("f1")));
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f0", "f2", "f3");

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f0"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(String.format("Cannot drop/rename partition key[%s]", "f0"));

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f2"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(String.format("Cannot drop/rename primary key[%s]", "f2"));
    }

    @Test
    public void testDropAllFields() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f0", "f1");

        schemaManager.commitChanges(Collections.singletonList(SchemaChange.dropColumn("f0")));
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f1");

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f100"))))
                .isInstanceOf(Catalog.ColumnNotExistException.class)
                .hasMessage(
                        String.format(
                                "Column %s does not exist in the %s table.",
                                "f100", identifier.getFullName()));

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot drop all fields in table");
    }

    @Test
    public void testCreateAlterSystemField() throws Exception {
        Schema schema2 =
                new Schema(
                        RowType.of(
                                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT()},
                                        new String[] {"f0", "_KEY_f1"})
                                .getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        assertThatThrownBy(() -> schemaManager.createTable(schema2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "Field name[%s] in schema cannot start with [%s]",
                                "_KEY_f1", KEY_FIELD_PREFIX));

        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.createTable(schema);

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.renameColumn("f0", "_VALUE_KIND"))))
                .satisfies(
                        anyCauseMatches(
                                RuntimeException.class,
                                String.format(
                                        "Field name[%s] in schema cannot be exist in %s",
                                        "_VALUE_KIND", SYSTEM_FIELD_NAMES)));
    }

    private List<String> readRecords(FileStoreTable table, Predicate filter) throws IOException {
        List<String> results = new ArrayList<>();
        forEachRemaining(
                table,
                filter,
                rowData ->
                        results.add(
                                DataFormatTestUtil.toStringNoRowKind(rowData, table.rowType())));
        return results;
    }

    private void forEachRemaining(
            FileStoreTable table, Predicate filter, Consumer<InternalRow> consumer)
            throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (filter != null) {
            snapshotReader.withFilter(filter);
        }
        for (Split split : snapshotReader.read().dataSplits()) {
            InnerTableRead read = table.newRead();
            if (filter != null) {
                read.withFilter(filter);
            }
            read.createReader(split).forEachRemaining(consumer);
        }
    }
}
