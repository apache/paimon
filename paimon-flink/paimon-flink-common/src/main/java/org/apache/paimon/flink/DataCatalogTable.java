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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link CatalogTable} to wrap {@link FileStoreTable}. */
public class DataCatalogTable implements CatalogTable {
    // Schema of the table (column names and types)
    private final Schema schema;

    // Partition keys if this is a partitioned table. It's an empty set if the table is not
    // partitioned
    private final List<String> partitionKeys;

    // Properties of the table
    private final Map<String, String> options;

    // Comment of the table
    private final String comment;

    private final Table table;
    private final Map<String, String> nonPhysicalColumnComments;

    public DataCatalogTable(
            Table table,
            Schema resolvedSchema,
            List<String> partitionKeys,
            Map<String, String> options,
            String comment,
            Map<String, String> nonPhysicalColumnComments) {
        this.schema = resolvedSchema;
        this.partitionKeys = checkNotNull(partitionKeys, "partitionKeys cannot be null");
        this.options = checkNotNull(options, "options cannot be null");

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "properties cannot have null keys or values");

        this.comment = comment;

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

        return toSchema(schema, columnComments);
    }

    private Schema toSchema(Schema tableSchema, Map<String, String> comments) {
        final Schema.Builder builder = Schema.newBuilder();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                                final Schema.UnresolvedPhysicalColumn c =
                                        (Schema.UnresolvedPhysicalColumn) column;
                                builder.column(c.getName(), c.getDataType());
                            } else if (column instanceof Schema.UnresolvedMetadataColumn) {
                                final Schema.UnresolvedMetadataColumn c =
                                        (Schema.UnresolvedMetadataColumn) column;
                                builder.columnByMetadata(
                                        c.getName(),
                                        c.getDataType(),
                                        c.getMetadataKey(),
                                        c.isVirtual());
                            } else if (column instanceof Schema.UnresolvedComputedColumn) {
                                final Schema.UnresolvedComputedColumn c =
                                        (Schema.UnresolvedComputedColumn) column;
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
                                        spec.getColumnName(), spec.getWatermarkExpression()));
        if (tableSchema.getPrimaryKey().isPresent()) {
            Schema.UnresolvedPrimaryKey primaryKey = tableSchema.getPrimaryKey().get();
            builder.primaryKeyNamed(primaryKey.getConstraintName(), primaryKey.getColumnNames());
        }
        return builder.build();
    }

    @Override
    public CatalogBaseTable copy() {
        return new DataCatalogTable(
                table,
                Schema.newBuilder().fromSchema(schema).build(),
                new ArrayList<>(getPartitionKeys()),
                new HashMap<>(getOptions()),
                getComment(),
                nonPhysicalColumnComments);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new DataCatalogTable(
                table,
                Schema.newBuilder().fromSchema(schema).build(),
                getPartitionKeys(),
                options,
                getComment(),
                nonPhysicalColumnComments);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a catalog table in an im-memory catalog");
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public String getComment() {
        return comment != null ? comment : "";
    }
}
