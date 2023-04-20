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

package org.apache.paimon.flink.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;

import java.util.Map;

/** Utility methods for {@link TableSchema}. */
public class FlinkTableSchemaUtils {

    /** Copied from {@link TableSchema#toSchema(Map)} to support versions lower than 1.17. */
    public static Schema toSchema(TableSchema tableSchema, Map<String, String> comments) {
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
}
