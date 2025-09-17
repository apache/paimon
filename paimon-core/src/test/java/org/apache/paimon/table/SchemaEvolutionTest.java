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
import org.apache.paimon.data.BinaryString;
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
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

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

import static org.apache.paimon.CoreOptions.AGG_FUNCTION;
import static org.apache.paimon.CoreOptions.DISTINCT;
import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.IGNORE_RETRACT;
import static org.apache.paimon.CoreOptions.NESTED_KEY;
import static org.apache.paimon.CoreOptions.SEQUENCE_FIELD;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
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

        schemaManager.commitChanges(
                Collections.singletonList(
                        SchemaChange.setOption("disable-explicit-type-casting", "true")));
        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.updateColumnType(
                                                        "f0", DataTypes.STRING()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Column type f0[BIGINT] cannot be converted to STRING without loosing information.");
        schemaManager.commitChanges(
                Collections.singletonList(
                        SchemaChange.setOption("disable-explicit-type-casting", "false")));
        // bigint to string
        tableSchema =
                schemaManager.commitChanges(
                        Collections.singletonList(
                                SchemaChange.updateColumnType("f0", DataTypes.STRING())));
        assertThat(tableSchema.fields().get(0).type()).isEqualTo(DataTypes.STRING());
    }

    @Test
    public void testUpdatePrimaryKeyType() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.BIGINT())
                        .primaryKey("k")
                        .build();
        schemaManager.createTable(schema);

        List<SchemaChange> changes =
                Collections.singletonList(SchemaChange.updateColumnType("k", DataTypes.STRING()));
        assertThatThrownBy(() -> schemaManager.commitChanges(changes))
                .hasMessageContaining("Cannot update primary key");
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
                                "f1", identifier.getFullName()));
    }

    @Test
    public void testRenamePrimaryKeyColumn() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Lists.newArrayList("f0"),
                        Lists.newArrayList("f0", "f1"),
                        Maps.newHashMap(),
                        "");

        schemaManager.createTable(schema);
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f0", "f1");

        schemaManager.commitChanges(SchemaChange.renameColumn("f1", "f1_"));
        TableSchema newSchema = schemaManager.latest().get();
        assertThat(newSchema.fieldNames()).containsExactly("f0", "f1_");
        assertThat(newSchema.primaryKeys()).containsExactlyInAnyOrder("f0", "f1_");

        assertThatThrownBy(
                        () -> schemaManager.commitChanges(SchemaChange.renameColumn("f0", "f0_")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Cannot rename partition column: [f0]");
    }

    @Test
    public void testRenameBucketKeyColumn() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        ImmutableList.of(),
                        Lists.newArrayList("f0", "f1"),
                        ImmutableMap.of(
                                CoreOptions.BUCKET_KEY.key(),
                                "f1,f0",
                                CoreOptions.BUCKET.key(),
                                "16"),
                        "");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.renameColumn("f0", "f0_"));
        TableSchema newSchema = schemaManager.latest().get();

        assertThat(newSchema.options().get(CoreOptions.BUCKET_KEY.key())).isEqualTo("f1,f0_");
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
                .hasMessage(String.format("Cannot drop partition key or primary key: [%s]", "f0"));

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f2"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(String.format("Cannot drop partition key or primary key: [%s]", "f2"));
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

    @Test
    public void testPushDownEvolutionSafeFilter() throws Exception {
        Schema schema =
                new Schema(
                        RowType.of(DataTypes.INT(), DataTypes.BIGINT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap("write-only", "true"),
                        "");
        schemaManager.createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        try (StreamTableWrite write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            // file 1
            write.write(GenericRow.of(1, 1L));
            commit.commit(1, write.prepareCommit(false, 1));

            // file 2
            write.write(GenericRow.of(2, 2L));
            commit.commit(2, write.prepareCommit(false, 2));
        }

        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.updateColumnType("f0", DataTypes.STRING())));
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        try (StreamTableWrite write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            // file 3
            write.write(GenericRow.of(BinaryString.fromString("0"), 3L));
            commit.commit(3, write.prepareCommit(false, 3));

            // file 4
            write.write(GenericRow.of(BinaryString.fromString("3"), 3L));
            commit.commit(4, write.prepareCommit(false, 4));
        }

        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        // p: f0 >= '3' && f1 >= 2L
        Predicate p =
                PredicateBuilder.and(
                        builder.greaterOrEqual(0, BinaryString.fromString("3")),
                        builder.greaterOrEqual(1, 2L));
        // file 1 will be filtered by f1 >= 2L
        // file 2 won't be filtered because f0 >= '3' is not safe
        // file 3 will be filtered by f0 >= '3'
        // file 4 won't be filtered
        List<String> rows = readRecords(table, p);
        assertThat(rows).containsExactlyInAnyOrder("2, 2", "3, 3");
    }

    @Test
    public void testRenameFieldReferencedByOptions() throws Exception {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        Schema schema =
                new Schema(
                        ImmutableList.of(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", DataTypes.INT()),
                                new DataField(3, "f3", DataTypes.INT()),
                                new DataField(
                                        4,
                                        "f4",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        new DataField(5, "f5", DataTypes.INT())))),
                                new DataField(6, "f6", DataTypes.ARRAY(DataTypes.INT()))),
                        ImmutableList.of("f0"),
                        ImmutableList.of(),
                        mapBuilder
                                .put(SEQUENCE_FIELD.key(), "f1,f2")
                                .put(FIELDS_PREFIX + "." + "f3" + "." + IGNORE_RETRACT, "true")
                                .put(
                                        FIELDS_PREFIX + "." + "f4" + "." + AGG_FUNCTION,
                                        "nested_update")
                                .put(FIELDS_PREFIX + "." + "f4" + "." + NESTED_KEY, "f5")
                                .put(FIELDS_PREFIX + "." + "f6" + "." + AGG_FUNCTION, "collect")
                                .put(FIELDS_PREFIX + "." + "f6" + "." + DISTINCT, "true")
                                .build(),
                        "");

        schemaManager.createTable(schema);

        TableSchema newSchema =
                schemaManager.commitChanges(
                        SchemaChange.renameColumn("f1", "f1_"),
                        SchemaChange.renameColumn("f2", "f2_"),
                        SchemaChange.renameColumn("f3", "f3_"),
                        SchemaChange.renameColumn("f4", "f4_"),
                        // doesn't support rename nested columns currently
                        // SchemaChange.renameColumn("f5", "f5_"),
                        SchemaChange.renameColumn("f6", "f6_"));

        assertThat(newSchema.fieldNames()).containsExactly("f0", "f1_", "f2_", "f3_", "f4_", "f6_");

        assertThat(newSchema.options())
                .doesNotContainKeys(
                        FIELDS_PREFIX + "." + "f3" + "." + IGNORE_RETRACT,
                        FIELDS_PREFIX + "." + "f4" + "." + AGG_FUNCTION,
                        FIELDS_PREFIX + "." + "f4" + "." + NESTED_KEY,
                        FIELDS_PREFIX + "." + "f6" + "." + AGG_FUNCTION,
                        FIELDS_PREFIX + "." + "f6" + "." + DISTINCT);

        Map.Entry[] entries =
                ImmutableMap.of(
                                SEQUENCE_FIELD.key(),
                                "f1_,f2_",
                                FIELDS_PREFIX + "." + "f3_" + "." + IGNORE_RETRACT,
                                "true",
                                FIELDS_PREFIX + "." + "f4_" + "." + AGG_FUNCTION,
                                "nested_update",
                                FIELDS_PREFIX + "." + "f6_" + "." + AGG_FUNCTION,
                                "collect",
                                FIELDS_PREFIX + "." + "f6_" + "." + DISTINCT,
                                "true")
                        .entrySet()
                        .toArray(new Map.Entry[0]);

        assertThat(newSchema.options()).contains(entries);
    }

    @Test
    public void testRenameSeqGroupFields() throws Exception {
        ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();

        Schema schema =
                new Schema(
                        ImmutableList.of(
                                new DataField(0, "f0", DataTypes.INT()),
                                new DataField(1, "f1", DataTypes.INT()),
                                new DataField(2, "f2", DataTypes.INT()),
                                new DataField(3, "f3", DataTypes.INT()),
                                new DataField(4, "f4", DataTypes.INT()),
                                new DataField(5, "f5", DataTypes.INT()),
                                new DataField(6, "f6", DataTypes.INT())),
                        ImmutableList.of("f0"),
                        ImmutableList.of(),
                        mapBuilder
                                .put(FIELDS_PREFIX + "." + "f1,f2" + "." + SEQUENCE_GROUP, "f3")
                                .put(FIELDS_PREFIX + "." + "f4" + "." + SEQUENCE_GROUP, "f5,f6")
                                .build(),
                        "");

        schemaManager.createTable(schema);

        TableSchema newSchema =
                schemaManager.commitChanges(
                        SchemaChange.renameColumn("f1", "f1_"),
                        SchemaChange.renameColumn("f2", "f2_"),
                        SchemaChange.renameColumn("f3", "f3_"),
                        SchemaChange.renameColumn("f4", "f4_"),
                        SchemaChange.renameColumn("f5", "f5_"),
                        SchemaChange.renameColumn("f6", "f6_"));

        assertThat(newSchema.fieldNames())
                .containsExactly("f0", "f1_", "f2_", "f3_", "f4_", "f5_", "f6_");

        assertThat(newSchema.options())
                .doesNotContainKeys(
                        FIELDS_PREFIX + "." + "f1,f2" + "." + SEQUENCE_GROUP,
                        FIELDS_PREFIX + "." + "f4" + "." + SEQUENCE_GROUP);

        Map.Entry[] entries =
                ImmutableMap.of(
                                FIELDS_PREFIX + "." + "f1_,f2_" + "." + SEQUENCE_GROUP, "f3_",
                                FIELDS_PREFIX + "." + "f4_" + "." + SEQUENCE_GROUP, "f5_,f6_")
                        .entrySet()
                        .toArray(new Map.Entry[0]);

        assertThat(newSchema.options()).contains(entries);
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
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();

        for (Split split : dataSplits) {
            InnerTableRead read = table.newRead();
            if (filter != null) {
                read.withFilter(filter);
            }
            read.createReader(split).forEachRemaining(consumer);
        }
    }
}
