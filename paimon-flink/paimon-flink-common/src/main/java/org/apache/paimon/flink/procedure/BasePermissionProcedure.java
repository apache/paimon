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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared REST catalog lookup and argument validation for permission procedures. */
abstract class BasePermissionProcedure extends ProcedureBase {

    private static final String COLUMN_NAMES_FORMAT_MESSAGE =
            "Column names must be a JSON array of strings, for example [\"col1\", \"col2\"].";

    protected PermissionManagement permissionManagement() {
        return restCatalog().permissionManagement();
    }

    private RESTCatalog restCatalog() {
        Catalog root = DelegateCatalog.rootCatalog(catalog);
        checkArgument(
                root instanceof RESTCatalog, "Catalog does not support permission management.");
        return (RESTCatalog) root;
    }

    protected static PermissionAssignment assignment(
            ResourceType resourceType,
            String access,
            String principal,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable PermissionColumns columns,
            @Nullable String expireTime) {
        return new PermissionAssignment(
                resource(resourceType, database, table, function, view),
                access,
                principal,
                columns,
                emptyToNull(expireTime));
    }

    protected static PermissionResource resource(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        return new PermissionResource(
                resourceType,
                emptyToNull(database),
                emptyToNull(table),
                emptyToNull(function),
                emptyToNull(view));
    }

    @Nullable
    protected static PermissionColumns columns(
            @Nullable String columnNames, @Nullable String excludedColumnNames) {
        List<String> included = parseColumnNames(columnNames);
        List<String> excluded = parseColumnNames(excludedColumnNames);
        return included == null && excluded == null
                ? null
                : new PermissionColumns(included, excluded);
    }

    protected static <E extends Enum<E>> E enumValue(
            String value, Class<E> enumClass, String argument) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(value), "%s cannot be empty.", argument);
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

    @Nullable
    protected static String[] toArray(@Nullable List<String> values) {
        return values == null ? null : values.toArray(new String[0]);
    }

    /**
     * Parses a JSON array of column names. Each string is kept exactly, including leading or
     * trailing spaces. The value must be one complete JSON array; trailing tokens are rejected so a
     * second array cannot be dropped before the grant.
     */
    @Nullable
    private static List<String> parseColumnNames(@Nullable String value) {
        String normalized = emptyToNull(value);
        if (normalized == null) {
            return null;
        }
        JsonNode node;
        try {
            node =
                    JsonSerdeUtil.OBJECT_MAPPER_INSTANCE
                            .readerFor(JsonNode.class)
                            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                            .readValue(normalized);
        } catch (IOException e) {
            throw new IllegalArgumentException(COLUMN_NAMES_FORMAT_MESSAGE, e);
        }
        checkArgument(node.isArray(), COLUMN_NAMES_FORMAT_MESSAGE);
        List<String> names = new ArrayList<>(node.size());
        for (JsonNode element : node) {
            checkArgument(element.isTextual(), "Column name must be a JSON string.");
            String name = element.asText();
            checkArgument(!name.isEmpty(), "Column name cannot be empty.");
            names.add(name);
        }
        return names;
    }

    @Nullable
    protected static String emptyToNull(@Nullable String value) {
        return StringUtils.isNullOrWhitespaceOnly(value) ? null : value;
    }
}
