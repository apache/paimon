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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Included or excluded top-level columns of a column-level permission assignment.
 *
 * <p>Exactly one list is present. Included names form an allowlist; excluded names form a denylist.
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PermissionColumns {

    private static final String FIELD_COLUMN_NAMES = "columnNames";
    private static final String FIELD_EXCLUDED_COLUMN_NAMES = "excludedColumnNames";

    @Nullable
    @JsonProperty(FIELD_COLUMN_NAMES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> columnNames;

    @Nullable
    @JsonProperty(FIELD_EXCLUDED_COLUMN_NAMES)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<String> excludedColumnNames;

    @JsonCreator
    @ConstructorProperties({FIELD_COLUMN_NAMES, FIELD_EXCLUDED_COLUMN_NAMES})
    public PermissionColumns(
            @Nullable @JsonProperty(FIELD_COLUMN_NAMES) List<String> columnNames,
            @Nullable @JsonProperty(FIELD_EXCLUDED_COLUMN_NAMES) List<String> excludedColumnNames) {
        checkArgument(
                (columnNames == null) != (excludedColumnNames == null),
                "columns must contain exactly one of columnNames or excludedColumnNames.");
        this.columnNames = immutableNonEmpty(columnNames, FIELD_COLUMN_NAMES);
        this.excludedColumnNames =
                immutableNonEmpty(excludedColumnNames, FIELD_EXCLUDED_COLUMN_NAMES);
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_NAMES)
    public List<String> getColumnNames() {
        return columnNames;
    }

    @Nullable
    @JsonGetter(FIELD_EXCLUDED_COLUMN_NAMES)
    public List<String> getExcludedColumnNames() {
        return excludedColumnNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PermissionColumns)) {
            return false;
        }
        PermissionColumns that = (PermissionColumns) o;
        return Objects.equals(columnNames, that.columnNames)
                && Objects.equals(excludedColumnNames, that.excludedColumnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnNames, excludedColumnNames);
    }

    @Nullable
    private static List<String> immutableNonEmpty(
            @Nullable List<String> columns, String fieldName) {
        if (columns == null) {
            return null;
        }
        checkArgument(!columns.isEmpty(), "%s cannot be empty.", fieldName);
        for (String column : columns) {
            checkArgument(
                    column != null && !column.trim().isEmpty(),
                    "%s cannot contain an empty column name.",
                    fieldName);
        }
        checkArgument(
                new HashSet<>(columns).size() == columns.size(),
                "%s cannot contain duplicate column names.",
                fieldName);
        return Collections.unmodifiableList(new ArrayList<>(columns));
    }
}
