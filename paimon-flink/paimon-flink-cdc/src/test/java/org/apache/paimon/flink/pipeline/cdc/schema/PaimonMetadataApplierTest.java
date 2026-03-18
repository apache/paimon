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

package org.apache.paimon.flink.pipeline.cdc.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.pipeline.cdc.util.FlinkCDCToPaimonTypeConverter;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Tests for {@link PaimonMetadataApplier}. */
class PaimonMetadataApplierTest {

    @TempDir public static java.nio.file.Path temporaryFolder;

    private static Catalog catalog;

    private static Options catalogOptions;

    private static MetadataApplier metadataApplier;

    @BeforeAll
    public static void initialize() {
        catalogOptions = new Options();
        catalogOptions.setString(
                CatalogOptions.METASTORE.key(), FileSystemCatalogFactory.IDENTIFIER);
        catalogOptions.setString(
                CatalogOptions.WAREHOUSE.key(),
                new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString());
        catalogOptions.set(CatalogOptions.CACHE_ENABLED.key(), "false");
        catalog = FlinkCatalogFactory.createPaimonCatalog(catalogOptions);

        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        metadataApplier = new PaimonMetadataApplier(catalogOptions, tableOptions, new HashMap<>());
    }

    @AfterAll
    public static void close() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        if (metadataApplier != null) {
            metadataApplier.close();
        }
    }

    @Test
    void testApplySchemaChange() throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "table1");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT())));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);
        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "col3",
                                org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                null,
                                "col3DefValue")));
        AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(
                                        2, "col3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);

        Map<String, String> nameMapping = new HashMap<>();
        nameMapping.put("col2", "newcol2");
        nameMapping.put("col3", "newcol3");
        RenameColumnEvent renameColumnEvent = new RenameColumnEvent(tableId, nameMapping);
        metadataApplier.applySchemaChange(renameColumnEvent);
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "newcol2", DataTypes.INT()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);

        Map<String, DataType> typeMapping = new HashMap<>();
        typeMapping.put("newcol2", org.apache.flink.cdc.common.types.DataTypes.STRING());
        AlterColumnTypeEvent alterColumnTypeEvent =
                new AlterColumnTypeEvent(TableId.parse(tableId.identifier()), typeMapping);
        metadataApplier.applySchemaChange(alterColumnTypeEvent);
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "newcol2", DataTypes.STRING()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);

        DropColumnEvent dropColumnEvent =
                new DropColumnEvent(tableId, Collections.singletonList("newcol2"));
        metadataApplier.applySchemaChange(dropColumnEvent);
        // id of DataField should keep the same as before dropping column
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(
                                        2, "newcol3", DataTypes.STRING(), null, "col3DefValue")));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);

        // Create table with partition column.
        tableId = TableId.tableId(databaseName, "table_with_partition");
        createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .physicalColumn(
                                        "dt",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .primaryKey("col1", "dt")
                                .partitionKey("dt")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(2, "dt", DataTypes.INT().notNull())));
        Table tableWithPartition = catalog.getTable(Identifier.fromString(tableId.identifier()));
        Assertions.assertThat(tableWithPartition.rowType()).isEqualTo(rowType);
        Assertions.assertThat(tableWithPartition.primaryKeys())
                .isEqualTo(Arrays.asList("col1", "dt"));
        // Create table with upper case.
        tableId = TableId.tableId(databaseName, "table_with_upper_case");
        catalogOptions.setString(CatalogOptions.CASE_SENSITIVE.key(), "true");
        PaimonMetadataApplier anotherMetadataApplier = new PaimonMetadataApplier(catalogOptions);
        createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "COL1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("COL1")
                                .build());
        anotherMetadataApplier.applySchemaChange(createTableEvent);
        rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "COL1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT())));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);
    }

    @Test
    public void testCreateTableWithoutPrimaryKey()
            throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "table1");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString(tableId.identifier()));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(3, "col4", DataTypes.STRING())));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEmpty();
        Assertions.assertThat(table.partitionKeys()).isEmpty();
        Assertions.assertThat(table.options()).containsEntry(CoreOptions.BUCKET.key(), "-1");
    }

    @Test
    void testCreateTableWithOptions() throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "table1");
        Map<TableId, List<String>> partitionMaps = new HashMap<>();
        partitionMaps.put(tableId, Arrays.asList("col3", "col4"));
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put(CoreOptions.BUCKET.key(), "-1");
        MetadataApplier anotherMetadataApplier =
                new PaimonMetadataApplier(catalogOptions, tableOptions, partitionMaps);
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .primaryKey("col1", "col3", "col4")
                                .build());
        anotherMetadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString(tableId.identifier()));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING().notNull()),
                                new DataField(3, "col4", DataTypes.STRING().notNull())));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEqualTo(Arrays.asList("col1", "col3", "col4"));
        Assertions.assertThat(table.partitionKeys()).isEqualTo(Arrays.asList("col3", "col4"));
        Assertions.assertThat(table.options()).containsEntry(CoreOptions.BUCKET.key(), "-1");
    }

    @Test
    void testCreateTableWithAllDataTypes()
            throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "table1");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "boolean",
                                        org.apache.flink.cdc.common.types.DataTypes.BOOLEAN())
                                .physicalColumn(
                                        "binary",
                                        org.apache.flink.cdc.common.types.DataTypes.BINARY(3))
                                .physicalColumn(
                                        "varbinary",
                                        org.apache.flink.cdc.common.types.DataTypes.VARBINARY(10))
                                .physicalColumn(
                                        "bytes",
                                        org.apache.flink.cdc.common.types.DataTypes.BYTES())
                                .physicalColumn(
                                        "tinyint",
                                        org.apache.flink.cdc.common.types.DataTypes.TINYINT())
                                .physicalColumn(
                                        "smallint",
                                        org.apache.flink.cdc.common.types.DataTypes.SMALLINT())
                                .physicalColumn(
                                        "int", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .physicalColumn(
                                        "float",
                                        org.apache.flink.cdc.common.types.DataTypes.FLOAT())
                                .physicalColumn(
                                        "double",
                                        org.apache.flink.cdc.common.types.DataTypes.DOUBLE())
                                .physicalColumn(
                                        "decimal",
                                        org.apache.flink.cdc.common.types.DataTypes.DECIMAL(6, 3))
                                .physicalColumn(
                                        "char", org.apache.flink.cdc.common.types.DataTypes.CHAR(5))
                                .physicalColumn(
                                        "varchar",
                                        org.apache.flink.cdc.common.types.DataTypes.VARCHAR(10))
                                .physicalColumn(
                                        "string",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .physicalColumn(
                                        "date", org.apache.flink.cdc.common.types.DataTypes.DATE())
                                .physicalColumn(
                                        "time", org.apache.flink.cdc.common.types.DataTypes.TIME())
                                .physicalColumn(
                                        "time_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIME(6))
                                .physicalColumn(
                                        "timestamp",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP())
                                .physicalColumn(
                                        "timestamp_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(3))
                                .physicalColumn(
                                        "timestamp_ltz",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ())
                                .physicalColumn(
                                        "timestamp_ltz_with_precision",
                                        org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(
                                                3))
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "boolean", DataTypes.BOOLEAN()),
                                new DataField(2, "binary", DataTypes.BINARY(3)),
                                new DataField(3, "varbinary", DataTypes.VARBINARY(10)),
                                new DataField(4, "bytes", DataTypes.BYTES()),
                                new DataField(5, "tinyint", DataTypes.TINYINT()),
                                new DataField(6, "smallint", DataTypes.SMALLINT()),
                                new DataField(7, "int", DataTypes.INT()),
                                new DataField(8, "float", DataTypes.FLOAT()),
                                new DataField(9, "double", DataTypes.DOUBLE()),
                                new DataField(10, "decimal", DataTypes.DECIMAL(6, 3)),
                                new DataField(11, "char", DataTypes.CHAR(5)),
                                new DataField(12, "varchar", DataTypes.VARCHAR(10)),
                                new DataField(13, "string", DataTypes.STRING()),
                                new DataField(14, "date", DataTypes.DATE()),
                                new DataField(15, "time", DataTypes.TIME(0)),
                                new DataField(16, "time_with_precision", DataTypes.TIME(6)),
                                new DataField(17, "timestamp", DataTypes.TIMESTAMP(6)),
                                new DataField(
                                        18, "timestamp_with_precision", DataTypes.TIMESTAMP(3)),
                                new DataField(
                                        19,
                                        "timestamp_ltz",
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)),
                                new DataField(
                                        20,
                                        "timestamp_ltz_with_precision",
                                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))));
        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(rowType);
    }

    @Test
    void testAddColumnWithPosition() throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "table1");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull())
                                .physicalColumn(
                                        "col2", org.apache.flink.cdc.common.types.DataTypes.INT())
                                .primaryKey("col1")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                new AddColumnEvent.ColumnWithPosition(
                        Column.physicalColumn(
                                "col3",
                                org.apache.flink.cdc.common.types.DataTypes
                                        .STRING()))); // default last position.
        AddColumnEvent addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(2, "col3", DataTypes.STRING())));

        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(tableSchema);

        addedColumns.clear();

        addedColumns.add(
                AddColumnEvent.before(
                        Column.physicalColumn(
                                "col4_first_before",
                                org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col1"));
        addedColumns.add(
                AddColumnEvent.first(
                        Column.physicalColumn(
                                "col4_first",
                                org.apache.flink.cdc.common.types.DataTypes.STRING())));
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "col5_last",
                                org.apache.flink.cdc.common.types.DataTypes.STRING())));
        addedColumns.add(
                AddColumnEvent.before(
                        Column.physicalColumn(
                                "col6_before",
                                org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col2"));
        addedColumns.add(
                AddColumnEvent.after(
                        Column.physicalColumn(
                                "col7_after", org.apache.flink.cdc.common.types.DataTypes.STRING()),
                        "col2"));

        addColumnEvent = new AddColumnEvent(tableId, addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(4, "col4_first", DataTypes.STRING()),
                                new DataField(3, "col4_first_before", DataTypes.STRING()),
                                new DataField(0, "col1", DataTypes.STRING().notNull()),
                                new DataField(6, "col6_before", DataTypes.STRING()),
                                new DataField(1, "col2", DataTypes.INT()),
                                new DataField(7, "col7_after", DataTypes.STRING()),
                                new DataField(2, "col3", DataTypes.STRING()),
                                new DataField(5, "col5_last", DataTypes.STRING())));

        Assertions.assertThat(
                        catalog.getTable(Identifier.fromString(tableId.identifier())).rowType())
                .isEqualTo(tableSchema);
    }

    @Test
    public void testCreateTableWithComment()
            throws Catalog.TableNotExistException, SchemaEvolveException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "test.table_with_comment");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "col1",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING()
                                                .notNull(),
                                        "comment of col1")
                                .physicalColumn(
                                        "col2",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col2")
                                .physicalColumn(
                                        "col3",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col3")
                                .physicalColumn(
                                        "col4",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING(),
                                        "comment of col4")
                                .comment("comment of table_with_comment")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);
        Table table = catalog.getTable(Identifier.fromString(tableId.identifier()));
        RowType tableSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0, "col1", DataTypes.STRING().notNull(), "comment of col1"),
                                new DataField(1, "col2", DataTypes.STRING(), "comment of col2"),
                                new DataField(2, "col3", DataTypes.STRING(), "comment of col3"),
                                new DataField(3, "col4", DataTypes.STRING(), "comment of col4")));
        Assertions.assertThat(table.rowType()).isEqualTo(tableSchema);
        Assertions.assertThat(table.primaryKeys()).isEmpty();
        Assertions.assertThat(table.partitionKeys()).isEmpty();
        Assertions.assertThat(table.options()).containsEntry("bucket", "-1");
        Assertions.assertThat(table.comment()).contains("comment of table_with_comment");
    }

    @Test
    public void testInvalidTimestampDefaultValueConversionInAddColumn()
            throws SchemaEvolveException, Catalog.TableNotExistException {
        String databaseName = "test_" + UUID.randomUUID();
        TableId tableId = TableId.tableId(databaseName, "timestamp_test");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        org.apache.flink.cdc.common.schema.Schema.newBuilder()
                                .physicalColumn(
                                        "id",
                                        org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                                .physicalColumn(
                                        "name",
                                        org.apache.flink.cdc.common.types.DataTypes.STRING())
                                .primaryKey("id")
                                .build());
        metadataApplier.applySchemaChange(createTableEvent);

        List<AddColumnEvent.ColumnWithPosition> addedColumns = new ArrayList<>();
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "created_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP(),
                                null,
                                FlinkCDCToPaimonTypeConverter.INVALID_OR_MISSING_DATETIME)));
        addedColumns.add(
                AddColumnEvent.last(
                        Column.physicalColumn(
                                "updated_time",
                                org.apache.flink.cdc.common.types.DataTypes.TIMESTAMP_LTZ(),
                                null,
                                FlinkCDCToPaimonTypeConverter.INVALID_OR_MISSING_DATETIME)));

        AddColumnEvent addColumnEvent =
                new AddColumnEvent(TableId.parse(tableId.identifier()), addedColumns);
        metadataApplier.applySchemaChange(addColumnEvent);

        Table table = catalog.getTable(Identifier.fromString(tableId.identifier()));

        Assertions.assertThat(table).isNotNull();
    }
}
