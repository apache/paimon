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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaChange;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderUtils;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.TableScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

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
        schemaManager = new SchemaManager(tablePath);
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

        FileStoreTable table = FileStoreTableFactory.create(tablePath);

        TableWrite write = table.newWrite(commitUser);
        write.write(GenericRowData.of(1, 1L));
        write.write(GenericRowData.of(2, 2L));
        table.newCommit(commitUser).commit(0, write.prepareCommit(true, 0));
        write.close();

        schemaManager.commitChanges(
                Collections.singletonList(SchemaChange.addColumn("f3", new BigIntType())));
        table = FileStoreTableFactory.create(tablePath);

        write = table.newWrite(commitUser);
        write.write(GenericRowData.of(3, 3L, 3L));
        write.write(GenericRowData.of(4, 4L, 4L));
        table.newCommit(commitUser).commit(1, write.prepareCommit(true, 1));
        write.close();

        // read all
        List<Row> rows = readRecords(table, null);
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, 1L, null),
                        Row.of(2, 2L, null),
                        Row.of(3, 3L, 3L),
                        Row.of(4, 4L, 4L));

        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());

        // read where f0 = 1 (filter on old field)
        rows = readRecords(table, builder.equal(0, 1));
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, 1L, null), Row.of(2, 2L, null));

        // read where f3 is null (filter on new field)
        rows = readRecords(table, builder.isNull(2));
        assertThat(rows).containsExactlyInAnyOrder(Row.of(1, 1L, null), Row.of(2, 2L, null));

        // read where f3 = 3 (filter on new field)
        rows = readRecords(table, builder.equal(2, 3L));
        assertThat(rows).containsExactlyInAnyOrder(Row.of(3, 3L, 3L), Row.of(4, 4L, 4L));
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

    private List<Row> readRecords(FileStoreTable table, Predicate filter) throws IOException {
        RowRowConverter converter =
                RowRowConverter.create(
                        TypeConversions.fromLogicalToDataType(table.schema().logicalRowType()));
        List<Row> results = new ArrayList<>();
        forEachRemaining(table, filter, rowData -> results.add(converter.toExternal(rowData)));
        return results;
    }

    private void forEachRemaining(
            FileStoreTable table, Predicate filter, Consumer<RowData> consumer) throws IOException {
        TableScan scan = table.newScan();
        if (filter != null) {
            scan.withFilter(filter);
        }
        for (Split split : scan.plan().splits()) {
            TableRead read = table.newRead();
            if (filter != null) {
                read.withFilter(filter);
            }
            RecordReader<RowData> reader = read.createReader(split);
            RecordReaderUtils.forEachRemaining(reader, consumer);
        }
    }
}
