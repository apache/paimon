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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Utilities for lake-stream mode. */
public class LakeStreamUtils {

    /** The {@code $rt} suffix, e.g. {@code my_table$rt}. */
    public static final String REAL_TIME_TABLE_SUFFIX = "rt";

    /**
     * Whether the identifier carries the {@code $rt} suffix. Branch access such as {@code
     * t1$branch_b1$rt} deliberately does not.
     */
    public static boolean isRealTimeTable(Identifier identifier) {
        return identifier.getBranchName() == null
                && REAL_TIME_TABLE_SUFFIX.equals(identifier.getSystemTableName());
    }

    public static boolean isRealTimeTable(ObjectPath tablePath) {
        return isRealTimeTable(FlinkCatalog.toIdentifier(tablePath));
    }

    /** Strips the {@code $rt} suffix, keeping catalog and database. */
    public static ObjectIdentifier stripRealTimeSuffix(ObjectIdentifier identifier) {
        Identifier parsed =
                new Identifier(identifier.getDatabaseName(), identifier.getObjectName());
        return ObjectIdentifier.of(
                identifier.getCatalogName(), identifier.getDatabaseName(), parsed.getTableName());
    }

    public static ObjectPath stripRealTimeSuffix(ObjectPath tablePath) {
        return new ObjectPath(
                tablePath.getDatabaseName(), FlinkCatalog.toIdentifier(tablePath).getTableName());
    }

    /**
     * Extracts every entry whose key starts with {@code prefix}, with the prefix removed, e.g.
     * {@code fluss.bootstrap.servers} becomes {@code bootstrap.servers} for the prefix {@code
     * fluss.}.
     */
    public static Map<String, String> extractPrefixedOptions(
            Map<String, String> options, String prefix) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    /** The persisted Paimon schema used for lake-stream compatibility checks. */
    public static Schema toPaimonSchema(Table table) {
        return new Schema(
                table.rowType().getFields(),
                table.partitionKeys(),
                table.primaryKeys(),
                table.options(),
                table.comment().orElse(null));
    }

    /** A store table expressed as a Paimon schema with only shared table options retained. */
    public static Schema toPaimonSchema(CatalogBaseTable table) {
        if (!(table instanceof CatalogTable)) {
            throw new ValidationException(
                    "Expected a table from the Lakestream Store, but got "
                            + table.getClass().getName());
        }
        org.apache.flink.table.api.Schema storeSchema = table.getUnresolvedSchema();
        Schema.Builder builder =
                Schema.newBuilder()
                        .partitionKeys(((CatalogTable) table).getPartitionKeys())
                        .options(sharedPaimonOptions(table.getOptions()))
                        .comment(table.getComment());
        for (org.apache.flink.table.api.Schema.UnresolvedColumn column : storeSchema.getColumns()) {
            if (column instanceof org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) {
                builder.column(
                        column.getName(),
                        toPaimonType(
                                ((org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn)
                                                column)
                                        .getDataType()),
                        column.getComment().orElse(null));
            }
        }
        storeSchema
                .getPrimaryKey()
                .ifPresent(primaryKey -> builder.primaryKey(primaryKey.getColumnNames()));
        return builder.build();
    }

    /** Whether physical fields, keys and shared immutable options match. */
    public static boolean isCompatible(Schema paimonSchema, Schema storeSchema) {
        return equalFields(paimonSchema.fields(), storeSchema.fields())
                && paimonSchema.partitionKeys().equals(storeSchema.partitionKeys())
                && paimonSchema.primaryKeys().equals(storeSchema.primaryKeys())
                && equalSelectedOptions(paimonSchema.options(), storeSchema.options());
    }

    private static org.apache.paimon.types.DataType toPaimonType(AbstractDataType<?> type) {
        return LogicalTypeConversion.toDataType(((DataType) type).getLogicalType());
    }

    private static Map<String, String> sharedPaimonOptions(Map<String, String> options) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("paimon.")) {
                key = key.substring("paimon.".length());
            }
            if (isComparedOption(key)) {
                result.put(key, entry.getValue());
            }
        }
        return result;
    }

    private static boolean equalFields(List<DataField> paimonFields, List<DataField> storeFields) {
        if (paimonFields.size() != storeFields.size()) {
            return false;
        }
        for (int i = 0; i < paimonFields.size(); i++) {
            DataField paimonField = paimonFields.get(i);
            DataField storeField = storeFields.get(i);
            if (!paimonField.name().equals(storeField.name())
                    || !equalPhysicalTypes(paimonField.type(), storeField.type())) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalPhysicalTypes(
            org.apache.paimon.types.DataType paimonType,
            org.apache.paimon.types.DataType storeType) {
        if (paimonType.isNullable() != storeType.isNullable()
                || paimonType.getTypeRoot() != storeType.getTypeRoot()) {
            return false;
        }
        if (paimonType instanceof RowType) {
            return storeType instanceof RowType
                    && equalFields(
                            ((RowType) paimonType).getFields(), ((RowType) storeType).getFields());
        }
        if (paimonType instanceof ArrayType) {
            return storeType instanceof ArrayType
                    && equalPhysicalTypes(
                            ((ArrayType) paimonType).getElementType(),
                            ((ArrayType) storeType).getElementType());
        }
        if (paimonType instanceof MultisetType) {
            return storeType instanceof MultisetType
                    && equalPhysicalTypes(
                            ((MultisetType) paimonType).getElementType(),
                            ((MultisetType) storeType).getElementType());
        }
        if (paimonType instanceof MapType) {
            return storeType instanceof MapType
                    && equalPhysicalTypes(
                            ((MapType) paimonType).getKeyType(), ((MapType) storeType).getKeyType())
                    && equalPhysicalTypes(
                            ((MapType) paimonType).getValueType(),
                            ((MapType) storeType).getValueType());
        }
        return paimonType.equals(storeType);
    }

    private static boolean equalSelectedOptions(
            Map<String, String> paimonOptions, Map<String, String> storeOptions) {
        for (String option : CoreOptions.IMMUTABLE_OPTIONS) {
            if (isComparedOption(option)
                    && !Objects.equals(paimonOptions.get(option), storeOptions.get(option))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isComparedOption(String key) {
        return CoreOptions.IMMUTABLE_OPTIONS.contains(key)
                && !CoreOptions.BUCKET_KEY.key().equals(key)
                && !CoreOptions.BUCKET_FUNCTION_TYPE.key().equals(key)
                && !CoreOptions.PRIMARY_KEY.key().equals(key)
                && !CoreOptions.PARTITION.key().equals(key);
    }
}
