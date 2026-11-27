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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.LazyField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunctionBase.ConvertAction.CONVERT;
import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

/** Adds missing source columns and widens compatible types before building a MySQL CDC job. */
final class MySqlStartupSchemaEvolution {

    static final String ADD_MISSING_COLUMNS_ON_STARTUP = "add_missing_columns_on_startup";

    private static final Logger LOG = LoggerFactory.getLogger(MySqlStartupSchemaEvolution.class);
    private static final int MAX_RETRIES = 2;

    private MySqlStartupSchemaEvolution() {}

    static FileStoreTable addMissingColumns(
            Catalog catalog,
            Identifier identifier,
            FileStoreTable table,
            Schema expected,
            Schema source,
            boolean caseSensitive,
            Map<String, String> tableConfig,
            TypeMapping typeMapping)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        for (int attempt = 0; ; attempt++) {
            List<SchemaChange> changes =
                    schemaChanges(
                            table.schema(),
                            expected,
                            source,
                            caseSensitive,
                            tableConfig,
                            typeMapping);
            if (changes.isEmpty()) {
                return table;
            }
            changes.add(0, SchemaChange.checkSchemaVersion(table.schema().id()));
            validateChanges(table, identifier, changes);
            try {
                catalog.alterTable(identifier, changes, false);
            } catch (Catalog.ColumnAlreadyExistException | ConcurrentModificationException e) {
                catalog.invalidateTable(identifier);
                table = loadTable(catalog, identifier);
                if (attempt == MAX_RETRIES) {
                    try {
                        if (schemaChanges(
                                        table.schema(),
                                        expected,
                                        source,
                                        caseSensitive,
                                        tableConfig,
                                        typeMapping)
                                .isEmpty()) {
                            return table;
                        }
                    } catch (IncompatibleSchemaException incompatible) {
                        incompatible.addSuppressed(e);
                        throw incompatible;
                    }
                    throw e;
                }
                continue;
            } catch (Catalog.TableNotExistException e) {
                throw tableDisappeared(identifier, e);
            }
            LOG.info(
                    "Applied MySQL startup schema changes {} to Paimon table {}.",
                    JsonSerdeUtil.toJson(changes.subList(1, changes.size())),
                    identifier);
            return loadTable(catalog, identifier);
        }
    }

    private static List<SchemaChange> schemaChanges(
            TableSchema target,
            Schema expected,
            Schema source,
            boolean caseSensitive,
            Map<String, String> tableConfig,
            TypeMapping typeMapping) {
        Set<String> sourceNames =
                source.fields().stream()
                        .map(field -> toLowerCaseIfNeed(field.name(), caseSensitive))
                        .collect(Collectors.toSet());
        Set<String> keyNames = new HashSet<>(target.primaryKeys());
        keyNames.addAll(target.partitionKeys());
        keyNames.addAll(target.bucketKeys());
        keyNames.addAll(listCaseConvert(source.primaryKeys(), caseSensitive));
        keyNames.addAll(expected.primaryKeys());
        keyNames.addAll(expected.partitionKeys());

        String bucketKey = tableConfig.get(CoreOptions.BUCKET_KEY.key());
        List<String> configuredBucketKeys =
                bucketKey == null
                        ? Collections.emptyList()
                        : Arrays.stream(bucketKey.split(","))
                                .map(String::trim)
                                .filter(key -> !key.isEmpty())
                                .collect(Collectors.toList());
        keyNames.addAll(configuredBucketKeys);

        if ((!expected.primaryKeys().isEmpty()
                        && !expected.primaryKeys().equals(target.primaryKeys()))
                || (!expected.partitionKeys().isEmpty()
                        && !expected.partitionKeys().equals(target.partitionKeys()))
                || (!configuredBucketKeys.isEmpty()
                        && !configuredBucketKeys.equals(target.bucketKeys()))) {
            throw new IncompatibleSchemaException(
                    "Startup schema evolution requires compatible primary, partition and bucket keys.");
        }

        Map<String, DataField> targetFields = target.nameToFieldMap();
        List<SchemaChange> changes = new ArrayList<>();
        for (DataField field : expected.fields()) {
            DataField oldField = targetFields.get(field.name());
            if (oldField != null
                    && (oldField.type().copy(true).equalsIgnoreFieldId(field.type().copy(true))
                            || canWiden(field.type(), oldField.type()))) {
                continue;
            }
            if (!sourceNames.contains(field.name()) || keyNames.contains(field.name())) {
                throw new IncompatibleSchemaException(
                        "Cannot automatically evolve key or generated field '"
                                + field.name()
                                + "'.");
            }
            if (oldField == null) {
                changes.add(
                        SchemaChange.addColumn(
                                field.name(), field.type().copy(true), columnComment(field)));
                continue;
            }

            DataType oldType = oldField.type();
            DataType sourceType = field.type();
            if (!canWiden(oldType, sourceType)
                    || (oldType instanceof DecimalType
                            && typeMapping.containsMode(
                                    TypeMapping.TypeMappingMode.DECIMAL_NO_CHANGE))) {
                throw new IncompatibleSchemaException(
                        String.format(
                                "Cannot safely evolve field '%s' from Paimon type %s to MySQL type %s.",
                                field.name(), oldType, sourceType));
            }
            changes.add(SchemaChange.updateColumnType(field.name(), sourceType, true));
        }
        return changes;
    }

    private static boolean canWiden(DataType from, DataType to) {
        DataType nullableFrom = from.copy(true);
        DataType nullableTo = to.copy(true);
        if (nullableFrom instanceof DecimalType && nullableTo instanceof DecimalType) {
            DecimalType fromDecimal = (DecimalType) nullableFrom;
            DecimalType toDecimal = (DecimalType) nullableTo;
            return toDecimal.getScale() >= fromDecimal.getScale()
                    && toDecimal.getPrecision() - toDecimal.getScale()
                            >= fromDecimal.getPrecision() - fromDecimal.getScale();
        }
        // Source types already include the configured mappings from JDBC schema discovery.
        // Match CdcActionCommonUtils.schemaCompatible so that startup evolution uses the same
        // conversion policy as the final compatibility check.
        return UpdatedDataFieldsProcessFunction.canConvert(
                        nullableFrom, nullableTo, TypeMapping.defaultMapping())
                == CONVERT;
    }

    private static void validateChanges(
            FileStoreTable table, Identifier identifier, List<SchemaChange> changes)
            throws Catalog.ColumnAlreadyExistException, Catalog.ColumnNotExistException {
        try {
            SchemaManager.generateTableSchema(
                    table.schema(),
                    changes,
                    new LazyField<>(() -> table.snapshotManager().latestSnapshot() != null),
                    new LazyField<>(() -> identifier));
        } catch (IllegalArgumentException
                | IllegalStateException
                | UnsupportedOperationException e) {
            throw new IncompatibleSchemaException(e.getMessage(), e);
        }
    }

    private static String columnComment(DataField field) {
        String comment = field.description();
        if (comment != null) {
            String trimmed = comment.trim();
            // Paimon interprets these prefixes as type conversion directives in ADD COLUMN.
            if (trimmed.startsWith("__BLOB")
                    || trimmed.startsWith("__VIDEO")
                    || trimmed.startsWith("__VECTOR")) {
                return null;
            }
        }
        return comment;
    }

    private static FileStoreTable loadTable(Catalog catalog, Identifier identifier) {
        try {
            return (FileStoreTable) catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw tableDisappeared(identifier, e);
        }
    }

    private static IllegalStateException tableDisappeared(
            Identifier identifier, Catalog.TableNotExistException cause) {
        return new IllegalStateException(
                "Paimon table " + identifier + " disappeared during CDC startup schema evolution.",
                cause);
    }

    static final class IncompatibleSchemaException extends IllegalArgumentException {

        private static final long serialVersionUID = 1L;

        IncompatibleSchemaException(String message) {
            this(message, null);
        }

        IncompatibleSchemaException(String message, Throwable cause) {
            super("Paimon schema and source table schema are not compatible. " + message, cause);
        }
    }
}
