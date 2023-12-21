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

package org.apache.paimon.flink;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** A {@link CatalogTableImpl} to wrap {@link FileStoreTable}. */
public class DataCatalogTable extends CatalogTableImpl {

    private final Table table;
    private final Map<String, String> nonPhysicalColumnComments;

    public DataCatalogTable(
            Table table,
            TableSchema tableSchema,
            List<String> partitionKeys,
            Map<String, String> properties,
            String comment,
            Map<String, String> nonPhysicalColumnComments) {
        super(tableSchema, partitionKeys, properties, comment);
        this.table = table;
        this.nonPhysicalColumnComments = nonPhysicalColumnComments;
    }

    public Table table() {
        return table;
    }

    @Override
    public Schema getUnresolvedSchema() {
        // add physical column comments
        Map<String, String> columnComments =
                table.rowType().getFields().stream()
                        .filter(dataField -> dataField.description() != null)
                        .collect(Collectors.toMap(DataField::name, DataField::description));

        return toSchema(getSchema(), columnComments);
    }

    /** Copied from {@link TableSchema#toSchema(Map)} to support versions lower than 1.17. */
    private Schema toSchema(TableSchema tableSchema, Map<String, String> comments) {
        final Schema.Builder builder = Schema.newBuilder();

        tableSchema
                .getTableColumns()
                .forEach(
                        column -> {
                            if (column instanceof TableColumn.PhysicalColumn) {
                                final TableColumn.PhysicalColumn c =
                                        (TableColumn.PhysicalColumn) column;
                                builder.column(c.getName(), c.getType());
                            } else if (column instanceof TableColumn.MetadataColumn) {
                                final TableColumn.MetadataColumn c =
                                        (TableColumn.MetadataColumn) column;
                                builder.columnByMetadata(
                                        c.getName(),
                                        c.getType(),
                                        c.getMetadataAlias().orElse(null),
                                        c.isVirtual());
                            } else if (column instanceof TableColumn.ComputedColumn) {
                                final TableColumn.ComputedColumn c =
                                        (TableColumn.ComputedColumn) column;
                                builder.columnByExpression(c.getName(), c.getExpression());
                            } else {
                                throw new IllegalArgumentException(
                                        "Unsupported column type: " + column);
                            }
                            String colName = column.getName();
                            if (comments.containsKey(colName)) {
                                builder.withComment(comments.get(colName));
                            } else if (nonPhysicalColumnComments.containsKey(colName)) {
                                builder.withComment(nonPhysicalColumnComments.get(colName));
                            }
                        });

        tableSchema
                .getWatermarkSpecs()
                .forEach(
                        spec ->
                                builder.watermark(
                                        spec.getRowtimeAttribute(), spec.getWatermarkExpr()));

        if (tableSchema.getPrimaryKey().isPresent()) {
            UniqueConstraint primaryKey = tableSchema.getPrimaryKey().get();
            builder.primaryKeyNamed(primaryKey.getName(), primaryKey.getColumns());
        }

        return builder.build();
    }

    @Override
    public CatalogBaseTable copy() {
        return new DataCatalogTable(
                table,
                getSchema().copy(),
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getComment(),
                nonPhysicalColumnComments);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new DataCatalogTable(
                table,
                getSchema(),
                getPartitionKeys(),
                options,
                getComment(),
                nonPhysicalColumnComments);
    }
}
