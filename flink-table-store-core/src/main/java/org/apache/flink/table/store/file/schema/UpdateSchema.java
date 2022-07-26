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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** A update schema. */
public class UpdateSchema {

    private final RowType rowType;

    private final List<String> partitionKeys;

    private final List<String> primaryKeys;

    private final Map<String, String> options;

    private final String comment;

    public UpdateSchema(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> options,
            String comment) {
        this.rowType = validateRowType(rowType, primaryKeys, partitionKeys);
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.options = new HashMap<>(options);
        this.comment = comment;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> options() {
        return options;
    }

    public String comment() {
        return comment;
    }

    private RowType validateRowType(
            RowType rowType, List<String> primaryKeys, List<String> partitionKeys) {
        List<String> fieldNames = rowType.getFieldNames();
        Set<String> allFields = new HashSet<>(fieldNames);
        Preconditions.checkState(
                allFields.containsAll(partitionKeys),
                "Table column %s should include all partition fields %s",
                fieldNames,
                partitionKeys);

        if (primaryKeys.isEmpty()) {
            return rowType;
        }
        Preconditions.checkState(
                allFields.containsAll(primaryKeys),
                "Table column %s should include all primary key constraint %s",
                fieldNames,
                primaryKeys);
        Set<String> pkSet = new HashSet<>(primaryKeys);
        Preconditions.checkState(
                pkSet.containsAll(partitionKeys),
                "Primary key constraint %s should include all partition fields %s",
                primaryKeys,
                partitionKeys);

        // primary key should not nullable
        List<RowType.RowField> fields = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            if (pkSet.contains(field.getName()) && field.getType().isNullable()) {
                fields.add(
                        new RowType.RowField(
                                field.getName(),
                                field.getType().copy(false),
                                field.getDescription().orElse(null)));
            } else {
                fields.add(field);
            }
        }
        return new RowType(false, fields);
    }

    @Override
    public String toString() {
        return "UpdateSchema{"
                + "rowType="
                + rowType
                + ", partitionKeys="
                + partitionKeys
                + ", primaryKeys="
                + primaryKeys
                + ", options="
                + options
                + ", comment="
                + comment
                + '}';
    }

    public CatalogTableImpl toCatalogTable() {
        TableSchema schema;
        Map<String, String> newOptions = new HashMap<>(options);

        // try to read schema from options
        // in the case of virtual columns and watermark
        DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
        tableSchemaProps.putProperties(newOptions);
        Optional<TableSchema> optional = tableSchemaProps.getOptionalTableSchema(Schema.SCHEMA);
        if (optional.isPresent()) {
            schema = optional.get();

            // remove schema from options
            DescriptorProperties removeProperties = new DescriptorProperties(false);
            removeProperties.putTableSchema(SCHEMA, schema);
            removeProperties.asMap().keySet().forEach(newOptions::remove);
        } else {
            TableSchema.Builder builder = TableSchema.builder();
            for (RowType.RowField field : rowType.getFields()) {
                builder.field(field.getName(), fromLogicalToDataType(field.getType()));
            }
            if (primaryKeys.size() > 0) {
                builder.primaryKey(primaryKeys.toArray(new String[0]));
            }

            schema = builder.build();
        }

        return new CatalogTableImpl(schema, partitionKeys, newOptions, comment);
    }

    public static UpdateSchema fromCatalogTable(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        List<String> primaryKeys = new ArrayList<>();
        if (schema.getPrimaryKey().isPresent()) {
            primaryKeys = schema.getPrimaryKey().get().getColumns();
        }

        Map<String, String> options = new HashMap<>(catalogTable.getOptions());

        // Serialize virtual columns and watermark to the options
        // This is what Flink SQL needs, the storage itself does not need them
        if (schema.getTableColumns().stream().anyMatch(c -> !c.isPhysical())
                || schema.getWatermarkSpecs().size() > 0) {
            DescriptorProperties tableSchemaProps = new DescriptorProperties(true);
            tableSchemaProps.putTableSchema(
                    org.apache.flink.table.descriptors.Schema.SCHEMA, schema);
            options.putAll(tableSchemaProps.asMap());
        }

        return new UpdateSchema(
                rowType,
                catalogTable.getPartitionKeys(),
                primaryKeys,
                options,
                catalogTable.getComment());
    }
}
