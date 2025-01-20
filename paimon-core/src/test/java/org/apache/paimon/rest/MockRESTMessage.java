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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterPartitionsRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.DropPartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableCredentialsResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.catalog.Catalog.COMMENT_PROP;

/** Mock REST message. */
public class MockRESTMessage {

    public static String databaseName() {
        return "database";
    }

    public static CreateDatabaseRequest createDatabaseRequest(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        return new CreateDatabaseRequest(name, options);
    }

    public static CreateDatabaseResponse createDatabaseResponse(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        return new CreateDatabaseResponse(name, options);
    }

    public static GetDatabaseResponse getDatabaseResponse(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        options.put(COMMENT_PROP, "comment");
        return new GetDatabaseResponse(UUID.randomUUID().toString(), name, options);
    }

    public static ListDatabasesResponse listDatabasesResponse(String name) {
        List<String> databaseNameList = new ArrayList<>();
        databaseNameList.add(name);
        return new ListDatabasesResponse(databaseNameList);
    }

    public static AlterDatabaseRequest alterDatabaseRequest() {
        Map<String, String> add = new HashMap<>();
        add.put("add", "value");
        return new AlterDatabaseRequest(Lists.newArrayList("remove"), add);
    }

    public static AlterDatabaseResponse alterDatabaseResponse() {
        return new AlterDatabaseResponse(
                Lists.newArrayList("remove"), Lists.newArrayList("add"), new ArrayList<>());
    }

    public static ListTablesResponse listTablesResponse() {
        return new ListTablesResponse(Lists.newArrayList("table"));
    }

    public static ListTablesResponse listTablesEmptyResponse() {
        return new ListTablesResponse(Lists.newArrayList());
    }

    public static CreateTableRequest createTableRequest(String name) {
        Identifier identifier = Identifier.create(databaseName(), name);
        Map<String, String> options = new HashMap<>();
        options.put("k1", "v1");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .options(options)
                        .build();
        return new CreateTableRequest(identifier, schema);
    }

    public static RenameTableRequest renameRequest(String sourceTable, String toTableName) {
        Identifier source = Identifier.create(databaseName(), sourceTable);
        Identifier destination = Identifier.create(databaseName(), toTableName);
        return new RenameTableRequest(source, destination);
    }

    public static AlterTableRequest alterTableRequest() {
        return new AlterTableRequest(getChanges());
    }

    public static CreatePartitionsRequest createPartitionRequest() {
        return new CreatePartitionsRequest(ImmutableList.of(Collections.singletonMap("pt", "1")));
    }

    public static DropPartitionsRequest dropPartitionsRequest() {
        return new DropPartitionsRequest(ImmutableList.of(Collections.singletonMap("pt", "1")));
    }

    public static ListPartitionsResponse listPartitionsResponse() {
        Map<String, String> spec = new HashMap<>();
        spec.put("f0", "1");
        Partition partition = new Partition(spec, 1, 1, 1, 1);
        return new ListPartitionsResponse(ImmutableList.of(partition));
    }

    public static List<SchemaChange> getChanges() {
        // add option
        SchemaChange addOption = SchemaChange.setOption("snapshot.time-retained", "2h");
        // remove option
        SchemaChange removeOption = SchemaChange.removeOption("compaction.max.file-num");
        // update comment
        SchemaChange updateComment = SchemaChange.updateComment(null);
        // add column
        SchemaChange addColumn =
                SchemaChange.addColumn("col1_after", DataTypes.ARRAY(DataTypes.STRING()));
        SchemaChange addColumnMap =
                SchemaChange.addColumn(
                        "col1_map_type", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()));
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT(),
                            DataTypes.BINARY(1),
                            DataTypes.VARBINARY(1),
                            DataTypes.MAP(DataTypes.VARCHAR(8), DataTypes.VARCHAR(8)),
                            DataTypes.MULTISET(DataTypes.VARCHAR(8))
                        },
                        new String[] {"pt", "a", "b", "c", "d", "e", "f"});
        SchemaChange addColumnRowType = SchemaChange.addColumn("col_row_type", rowType);
        // add a column after col1
        SchemaChange.Move after = SchemaChange.Move.after("col1_after", "col1");
        SchemaChange addColumnAfterField =
                SchemaChange.addColumn("col7", DataTypes.STRING(), "", after);
        // rename column
        SchemaChange renameColumn = SchemaChange.renameColumn("col3", "col3_new_name");
        // drop column
        SchemaChange dropColumn = SchemaChange.dropColumn("col6");
        // update column comment
        SchemaChange updateColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col4"}, "col4 field");
        // update nested column comment
        SchemaChange updateNestedColumnComment =
                SchemaChange.updateColumnComment(new String[] {"col5", "f1"}, "col5 f1 field");
        // update column type
        SchemaChange updateColumnType = SchemaChange.updateColumnType("col4", DataTypes.DOUBLE());
        // update column position, you need to pass in a parameter of type Move
        SchemaChange updateColumnPosition =
                SchemaChange.updateColumnPosition(SchemaChange.Move.first("col4"));
        // update column nullability
        SchemaChange updateColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col4"}, false);
        // update nested column nullability
        SchemaChange updateNestedColumnNullability =
                SchemaChange.updateColumnNullability(new String[] {"col5", "f2"}, false);

        List<SchemaChange> schemaChanges = new ArrayList<>();
        schemaChanges.add(addOption);
        schemaChanges.add(removeOption);
        schemaChanges.add(updateComment);
        schemaChanges.add(addColumn);
        schemaChanges.add(addColumnMap);
        schemaChanges.add(addColumnRowType);
        schemaChanges.add(addColumnAfterField);
        schemaChanges.add(renameColumn);
        schemaChanges.add(dropColumn);
        schemaChanges.add(updateColumnComment);
        schemaChanges.add(updateNestedColumnComment);
        schemaChanges.add(updateColumnType);
        schemaChanges.add(updateColumnPosition);
        schemaChanges.add(updateColumnNullability);
        schemaChanges.add(updateNestedColumnNullability);
        return schemaChanges;
    }

    public static GetTableResponse getTableResponse() {
        Map<String, String> options = new HashMap<>();
        options.put("option-1", "value-1");
        options.put("option-2", "value-2");
        return new GetTableResponse(UUID.randomUUID().toString(), "", 1, schema(options));
    }

    public static AlterPartitionsRequest alterPartitionsRequest() {
        return new AlterPartitionsRequest(ImmutableList.of(partition()));
    }

    public static CreateViewRequest createViewRequest(String name) {
        Identifier identifier = Identifier.create(databaseName(), name);
        return new CreateViewRequest(identifier, viewSchema());
    }

    public static GetViewResponse getViewResponse() {
        return new GetViewResponse(UUID.randomUUID().toString(), "", viewSchema());
    }

    public static ListViewsResponse listViewsResponse() {
        return new ListViewsResponse(ImmutableList.of("view"));
    }

    public static GetTableCredentialsResponse getTableCredentialsResponse() {
        return new GetTableCredentialsResponse(
                System.currentTimeMillis(), ImmutableMap.of("key", "value"));
    }

    private static ViewSchema viewSchema() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()));
        return new ViewSchema(
                new RowType(fields),
                Collections.singletonMap("pt", "1"),
                "comment",
                "select * from t1");
    }

    private static Partition partition() {
        return new Partition(Collections.singletonMap("pt", "1"), 1, 1, 1, 1);
    }

    private static Schema schema(Map<String, String> options) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()));
        List<String> partitionKeys = Collections.singletonList("f0");
        List<String> primaryKeys = Arrays.asList("f0", "f1");
        return new Schema(fields, partitionKeys, primaryKeys, options, "comment");
    }
}
