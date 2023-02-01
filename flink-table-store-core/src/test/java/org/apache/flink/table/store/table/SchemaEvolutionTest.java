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

package org.apache.flink.table.store.table;

import org.apache.flink.table.store.data.DataFormatTestUtil;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderUtils;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.FloatType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.flink.table.store.file.schema.TableSchema.KEY_FIELD_PREFIX;
import static org.apache.flink.table.store.file.schema.TableSchema.SYSTEM_FIELD_NAMES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for schema evolution. */
public class SchemaEvolutionTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private SchemaManager schemaManager;
    private String commitUser;

    @BeforeEach
    public void beforeEach() {
        tablePath = new Path(tempDir.toUri());
        schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testAddField() throws Exception {
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        TableWrite write = table.newWrite(commitUser);
        write.write(GenericRow.of(1, 1L));
        write.write(GenericRow.of(2, 2L));
        table.newCommit(commitUser).commit(0, write.prepareCommit(true, 0));
        write.close();

        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.addColumn("f3", new BigIntType())));
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        write = table.newWrite(commitUser);
        write.write(GenericRow.of(3, 3L, 3L));
        write.write(GenericRow.of(4, 4L, 4L));
        table.newCommit(commitUser).commit(1, write.prepareCommit(true, 1));
        write.close();

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
                                                        "f4", new IntType().copy(false), null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ADD COLUMN cannot specify NOT NULL.");
    }

    @Test
    public void testAddDuplicateField() throws Exception {
        final String columnName = "f3";
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);
        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.addColumn(columnName, new BigIntType())));
        assertThatThrownBy(
                        () -> {
                            schemaManager.commitChanges(
                                    Collections.singletonList(
                                            SchemaChange.addColumn(columnName, new FloatType())));
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The column [%s] exists in the table[%s].", columnName, tablePath);
    }

    @Test
    public void testUpdateFieldType() throws Exception {
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);

        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.updateColumnType("f0", new BigIntType())));
        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.updateColumnType(
                                                        "f0", new IntType()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "Column type %s[%s] cannot be converted to %s without loosing information.",
                                "f0", new BigIntType(), new IntType()));
    }

    @Test
    public void testRenameField() throws Exception {
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);
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
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format("The column [%s] exists in the table[%s].", "f1", tablePath));
    }

    @Test
    public void testDropField() throws Exception {
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(
                                new IntType(), new BigIntType(), new IntType(), new BigIntType()),
                        Collections.singletonList("f0"),
                        Arrays.asList("f0", "f2"),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);
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
        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f0", "f1");

        schemaManager.commitChanges(Collections.singletonList(SchemaChange.dropColumn("f0")));
        assertThat(schemaManager.latest().get().fieldNames()).containsExactly("f1");

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f100"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        String.format(
                                "The column [%s] doesn't exist in the table[%s].",
                                "f100", tablePath));

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(SchemaChange.dropColumn("f1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot drop all fields in table");
    }

    @Test
    public void testCreateAlterSystemField() throws Exception {
        UpdateSchema updateSchema1 =
                new UpdateSchema(
                        RowType.of(
                                new DataType[] {new IntType(), new BigIntType()},
                                new String[] {"f0", "_VALUE_COUNT"}),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        assertThatThrownBy(() -> schemaManager.commitNewVersion(updateSchema1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "Field name[%s] in schema cannot be exist in [%s]",
                                "_VALUE_COUNT", SYSTEM_FIELD_NAMES.toString()));

        UpdateSchema updateSchema2 =
                new UpdateSchema(
                        RowType.of(
                                new DataType[] {new IntType(), new BigIntType()},
                                new String[] {"f0", "_KEY_f1"}),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        assertThatThrownBy(() -> schemaManager.commitNewVersion(updateSchema2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "Field name[%s] in schema cannot start with [%s]",
                                "_KEY_f1", KEY_FIELD_PREFIX));

        UpdateSchema updateSchema =
                new UpdateSchema(
                        RowType.of(new IntType(), new BigIntType()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        "");
        schemaManager.commitNewVersion(updateSchema);

        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Collections.singletonList(
                                                SchemaChange.renameColumn("f0", "_VALUE_KIND"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        String.format(
                                "Field name[%s] in schema cannot be exist in [%s]",
                                "_VALUE_KIND", SYSTEM_FIELD_NAMES.toString()));
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
        TableScan scan = table.newScan();
        if (filter != null) {
            scan.withFilter(filter);
        }
        for (Split split : scan.plan().splits()) {
            TableRead read = table.newRead();
            if (filter != null) {
                read.withFilter(filter);
            }
            RecordReader<InternalRow> reader = read.createReader(split);
            RecordReaderUtils.forEachRemaining(reader, consumer);
        }
    }
}
