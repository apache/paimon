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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.ColumnSelection;
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared REST catalog lookup and argument validation for permission procedures. */
abstract class BasePermissionProcedure extends BaseProcedure {

    protected BasePermissionProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    protected PermissionManagement permissionManagement() {
        checkArgument(
                tableCatalog() instanceof WithPaimonCatalog,
                "Catalog '%s' is not a Paimon catalog.",
                tableCatalog().name());
        Catalog root =
                DelegateCatalog.rootCatalog(((WithPaimonCatalog) tableCatalog()).paimonCatalog());
        checkArgument(
                root instanceof RESTCatalog,
                "Catalog '%s' does not support permission management.",
                tableCatalog().name());
        return ((RESTCatalog) root).permissionManagement();
    }

    protected static Permission permission(
            ResourceType resourceType,
            String access,
            String principal,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable ArrayData columnNames,
            @Nullable ArrayData excludedColumnNames,
            @Nullable String rowFilter,
            @Nullable MapData columnMasking,
            @Nullable String expireTime) {
        database = emptyToNull(database);
        table = emptyToNull(table);
        function = emptyToNull(function);
        view = emptyToNull(view);
        access = requiredUpperCase(access, "access");
        checkPrincipal(principal);
        validateResource(resourceType, database, table, function, view);

        List<String> included = toStrings(columnNames, "column_names");
        List<String> excluded = toStrings(excludedColumnNames, "excluded_column_names");
        checkArgument(
                included == null || excluded == null,
                "Only one of column_names and excluded_column_names may be specified.");
        ColumnSelection columns =
                included == null && excluded == null
                        ? null
                        : new ColumnSelection(included, excluded);

        rowFilter = emptyToNull(rowFilter);
        Map<String, ColumnMask> masks = toColumnMasks(columnMasking);
        validatePolicy(resourceType, access, columns, rowFilter, masks, expireTime);

        return new Permission(
                resourceType,
                null,
                database,
                table,
                function,
                view,
                columns,
                rowFilter == null ? null : new RowFilter(rowFilter, null),
                masks,
                access,
                principal,
                emptyToNull(expireTime));
    }

    protected static Permission revokePermission(
            ResourceType resourceType,
            String access,
            String principal,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        database = emptyToNull(database);
        table = emptyToNull(table);
        function = emptyToNull(function);
        view = emptyToNull(view);
        validateResource(resourceType, database, table, function, view);
        checkPrincipal(principal);
        return new Permission(
                resourceType,
                null,
                database,
                table,
                function,
                view,
                null,
                null,
                null,
                requiredUpperCase(access, "access"),
                principal,
                null);
    }

    private static void validateResource(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        switch (resourceType) {
            case CATALOG:
            case CATALOG_ALL:
                checkArgument(
                        database == null && table == null && function == null && view == null,
                        "%s resource cannot specify database, table, function, or view.",
                        resourceType);
                break;
            case DATABASE:
            case DATABASE_ALL:
                checkArgument(database != null, "%s resource requires database.", resourceType);
                checkArgument(
                        table == null && function == null && view == null,
                        "%s resource cannot specify table, function, or view.",
                        resourceType);
                break;
            case TABLE:
            case COLUMN:
            case ROW_FILTER:
            case COLUMN_MASKING:
                checkArgument(
                        database != null && table != null,
                        "%s resource requires database and table.",
                        resourceType);
                checkArgument(
                        function == null && view == null,
                        "%s resource cannot specify function or view.",
                        resourceType);
                break;
            case FUNCTION:
                checkArgument(
                        database != null && function != null,
                        "FUNCTION resource requires database and function.");
                checkArgument(
                        table == null && view == null, "FUNCTION cannot specify table or view.");
                break;
            case VIEW:
                checkArgument(
                        database != null && view != null,
                        "VIEW resource requires database and view.");
                checkArgument(
                        table == null && function == null,
                        "VIEW cannot specify table or function.");
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }
    }

    private static void validatePolicy(
            ResourceType resourceType,
            String access,
            @Nullable ColumnSelection columns,
            @Nullable String rowFilter,
            @Nullable Map<String, ColumnMask> columnMasking,
            @Nullable String expireTime) {
        if (resourceType == ResourceType.COLUMN) {
            checkArgument(
                    "SELECT".equals(access), "COLUMN permission only supports SELECT access.");
            checkArgument(
                    columns != null,
                    "COLUMN permission requires column_names or excluded_column_names.");
        } else {
            checkArgument(columns == null, "Columns are supported only for COLUMN permissions.");
        }

        if (resourceType == ResourceType.ROW_FILTER) {
            checkArgument(
                    "ROW_FILTER".equals(access),
                    "ROW_FILTER permission requires ROW_FILTER access.");
            checkArgument(rowFilter != null, "ROW_FILTER permission requires row_filter.");
            checkArgument(
                    emptyToNull(expireTime) == null,
                    "expire_time is not supported for ROW_FILTER permissions.");
        } else {
            checkArgument(
                    rowFilter == null, "row_filter is supported only for ROW_FILTER permissions.");
        }

        if (resourceType == ResourceType.COLUMN_MASKING) {
            checkArgument(
                    "COLUMN_MASKING".equals(access),
                    "COLUMN_MASKING permission requires COLUMN_MASKING access.");
            checkArgument(
                    columnMasking != null, "COLUMN_MASKING permission requires column_masking.");
            checkArgument(
                    emptyToNull(expireTime) == null,
                    "expire_time is not supported for COLUMN_MASKING permissions.");
        } else {
            checkArgument(
                    columnMasking == null,
                    "column_masking is supported only for COLUMN_MASKING permissions.");
        }
    }

    protected static <E extends Enum<E>> E enumValue(
            String value, Class<E> enumClass, String argument) {
        checkArgument(!isBlank(value), "%s cannot be empty.", argument);
        try {
            return Enum.valueOf(enumClass, value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid %s '%s'. Expected one of %s.",
                            argument, value, Arrays.toString(enumClass.getEnumConstants())),
                    e);
        }
    }

    protected static void checkPrincipal(String principal) {
        checkArgument(!isBlank(principal), "principal cannot be empty.");
    }

    private static String requiredUpperCase(String value, String argument) {
        checkArgument(!isBlank(value), "%s cannot be empty.", argument);
        return value.toUpperCase(Locale.ROOT);
    }

    @Nullable
    private static List<String> toStrings(@Nullable ArrayData data, String argument) {
        if (data == null) {
            return null;
        }
        checkArgument(data.numElements() > 0, "%s cannot be empty.", argument);
        List<String> result = new ArrayList<>(data.numElements());
        for (int i = 0; i < data.numElements(); i++) {
            checkArgument(!data.isNullAt(i), "%s cannot contain null.", argument);
            String value = data.getUTF8String(i).toString();
            checkArgument(!isBlank(value), "%s cannot contain empty values.", argument);
            result.add(value);
        }
        return result;
    }

    @Nullable
    private static Map<String, ColumnMask> toColumnMasks(@Nullable MapData data) {
        if (data == null) {
            return null;
        }
        checkArgument(data.numElements() > 0, "column_masking cannot be empty.");
        Map<String, ColumnMask> result = new HashMap<>();
        for (int i = 0; i < data.numElements(); i++) {
            checkArgument(
                    !data.keyArray().isNullAt(i) && !data.valueArray().isNullAt(i),
                    "column_masking cannot contain null keys or values.");
            String key = data.keyArray().getUTF8String(i).toString();
            String value = data.valueArray().getUTF8String(i).toString();
            checkArgument(
                    !isBlank(key) && !isBlank(value),
                    "column_masking cannot contain empty keys or values.");
            result.put(key, new ColumnMask(value, null));
        }
        return result;
    }

    @Nullable
    protected static String emptyToNull(@Nullable String value) {
        return isBlank(value) ? null : value;
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
