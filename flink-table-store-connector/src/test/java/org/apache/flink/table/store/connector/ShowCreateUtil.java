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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * SHOW CREATE statement Util.
 *
 * <p>This code is mostly copied from {@link org.apache.flink.table.api.internal.ShowCreateUtil}.
 */
public class ShowCreateUtil {

    private ShowCreateUtil() {}

    public static String createTableLikeDDL(
            String sourceTableName,
            String managedTableName,
            Map<String, String> tableOptions,
            @Nullable ReadWriteTableTestBase.WatermarkSpec watermarkSpec) {
        StringBuilder ddl =
                new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                        .append(String.format("`%s`", managedTableName));
        if (watermarkSpec != null) {
            ddl.append(
                    String.format(
                            "(\n WATERMARK FOR %s AS %s)\n",
                            watermarkSpec.columnName, watermarkSpec.expressionAsString));
        }
        ddl.append(optionsToSql(tableOptions))
                .append(String.format(" LIKE `%s` (EXCLUDING OPTIONS)\n", sourceTableName));
        return ddl.toString();
    }

    public static String buildInsertIntoQuery(String sourceTableName, String managedTableName) {
        return buildInsertIntoQuery(
                sourceTableName, managedTableName, Collections.emptyMap(), Collections.emptyMap());
    }

    public static String buildInsertIntoQuery(
            String sourceTableName,
            String managedTableName,
            Map<String, String> partitions,
            Map<String, String> hints) {
        StringBuilder insertDmlBuilder =
                new StringBuilder(String.format("INSERT INTO `%s`", managedTableName));
        if (partitions.size() > 0) {
            insertDmlBuilder.append(" PARTITION (");
            partitions.forEach(
                    (k, v) -> {
                        insertDmlBuilder.append(String.format("'%s'", k));
                        insertDmlBuilder.append(" = ");
                        insertDmlBuilder.append(String.format("'%s', ", v));
                    });
            int len = insertDmlBuilder.length();
            insertDmlBuilder.deleteCharAt(len - 1);
            insertDmlBuilder.append(")");
        }
        insertDmlBuilder.append(String.format("\n SELECT * FROM `%s`", sourceTableName));
        insertDmlBuilder.append(buildHints(hints));

        return insertDmlBuilder.toString();
    }

    public static String buildSelectQuery(
            String tableName,
            Map<String, String> hints,
            @Nullable String filter,
            List<String> projections) {
        StringBuilder queryBuilder =
                new StringBuilder(
                        String.format(
                                "SELECT %s FROM `%s` %s",
                                projections.isEmpty() ? "*" : String.join(", ", projections),
                                tableName,
                                buildHints(hints)));
        if (filter != null) {
            queryBuilder.append("\nWHERE ").append(filter);
        }
        return queryBuilder.toString();
    }

    public static String buildShowCreateTable(
            ResolvedCatalogBaseTable<?> table,
            ObjectIdentifier tableIdentifier,
            boolean ignoreIfExists) {
        if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            throw new TableException(
                    String.format(
                            "SHOW CREATE TABLE is only supported for tables, but %s is a view. Please use SHOW CREATE VIEW instead.",
                            tableIdentifier.asSerializableString()));
        }
        final String printIndent = "  ";
        StringBuilder sb =
                new StringBuilder()
                        .append(buildCreateFormattedPrefix(ignoreIfExists, tableIdentifier));
        sb.append(extractFormattedColumns(table, printIndent));
        extractFormattedWatermarkSpecs(table, printIndent)
                .ifPresent(watermarkSpecs -> sb.append(",\n").append(watermarkSpecs));
        extractFormattedPrimaryKey(table, printIndent).ifPresent(pk -> sb.append(",\n").append(pk));
        sb.append("\n) ");
        extractFormattedComment(table)
                .ifPresent(
                        c -> sb.append(String.format("COMMENT '%s'%s", c, System.lineSeparator())));
        extractFormattedPartitionedInfo((ResolvedCatalogTable) table)
                .ifPresent(
                        partitionedInfoFormatted ->
                                sb.append("PARTITIONED BY (")
                                        .append(partitionedInfoFormatted)
                                        .append(")\n"));
        extractFormattedOptions(table, printIndent)
                .ifPresent(v -> sb.append("WITH (\n").append(v).append("\n)\n"));
        return sb.toString();
    }

    private static String extractFormattedColumns(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        return table.getResolvedSchema().getColumns().stream()
                .map(column -> String.format("%s%s", printIndent, getColumnString(column)))
                .collect(Collectors.joining(",\n"));
    }

    private static Optional<String> extractFormattedWatermarkSpecs(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        if (table.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                table.getResolvedSchema().getWatermarkSpecs().stream()
                        .map(
                                watermarkSpec ->
                                        String.format(
                                                "%sWATERMARK FOR %s AS %s",
                                                printIndent,
                                                EncodingUtils.escapeIdentifier(
                                                        watermarkSpec.getRowtimeAttribute()),
                                                watermarkSpec
                                                        .getWatermarkExpression()
                                                        .asSerializableString()))
                        .collect(Collectors.joining("\n")));
    }

    private static Optional<String> extractFormattedComment(ResolvedCatalogBaseTable<?> table) {
        String comment = table.getComment();
        if (StringUtils.isNotEmpty(comment)) {
            return Optional.of(EncodingUtils.escapeSingleQuotes(comment));
        }
        return Optional.empty();
    }

    private static Optional<String> extractFormattedPartitionedInfo(
            ResolvedCatalogTable catalogTable) {
        if (!catalogTable.isPartitioned()) {
            return Optional.empty();
        }
        return Optional.of(
                catalogTable.getPartitionKeys().stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")));
    }

    private static Optional<String> extractFormattedOptions(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        if (Objects.isNull(table.getOptions()) || table.getOptions().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                table.getOptions().entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "%s'%s' = '%s'",
                                                printIndent,
                                                EncodingUtils.escapeSingleQuotes(entry.getKey()),
                                                EncodingUtils.escapeSingleQuotes(entry.getValue())))
                        .collect(Collectors.joining(",\n")));
    }

    private static String buildCreateFormattedPrefix(
            boolean ignoreIfExists, ObjectIdentifier identifier) {
        return String.format(
                "CREATE TABLE%s %s (%s",
                ignoreIfExists ? " IF NOT EXISTS " : "",
                identifier.asSerializableString(),
                System.lineSeparator());
    }

    private static Optional<String> extractFormattedPrimaryKey(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        Optional<UniqueConstraint> primaryKey = table.getResolvedSchema().getPrimaryKey();
        return primaryKey.map(
                uniqueConstraint -> String.format("%s%s", printIndent, uniqueConstraint));
    }

    private static String getColumnString(Column column) {
        final StringBuilder sb = new StringBuilder();
        sb.append(EncodingUtils.escapeIdentifier(column.getName()));
        sb.append(" ");
        // skip data type for computed column
        if (column instanceof Column.ComputedColumn) {
            sb.append(
                    column.explainExtras()
                            .orElseThrow(
                                    () ->
                                            new TableException(
                                                    String.format(
                                                            "Column expression can not be null for computed column '%s'",
                                                            column.getName()))));
        } else {
            sb.append(column.getDataType().getLogicalType().asSerializableString());
            column.explainExtras()
                    .ifPresent(
                            e -> {
                                sb.append(" ");
                                sb.append(e);
                            });
        }
        // TODO: Print the column comment until FLINK-18958 is fixed
        return sb.toString();
    }

    private static String optionsToSql(Map<String, String> tableOptions) {
        String optionsStr =
                tableOptions.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "'%s' = '%s'", entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(",\n"));
        if (StringUtils.isNotEmpty(optionsStr)) {
            return "WITH (\n  " + optionsStr + ")";
        }
        return optionsStr;
    }

    private static String buildHints(Map<String, String> hints) {
        if (hints.size() > 0) {
            String hintString =
                    hints.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "'%s' = '%s'",
                                                    entry.getKey(), entry.getValue()))
                            .collect(Collectors.joining(", "));
            return "/*+ OPTIONS (" + hintString + ") */";
        }
        return "";
    }
}
