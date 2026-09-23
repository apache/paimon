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
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Structured reference to a resource or explicit descendant scope inside the REST catalog. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PermissionResource {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_VIEW = "view";

    @JsonProperty(FIELD_TYPE)
    private final ResourceType type;

    @Nullable
    @JsonProperty(FIELD_DATABASE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String database;

    @Nullable
    @JsonProperty(FIELD_TABLE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String table;

    @Nullable
    @JsonProperty(FIELD_FUNCTION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String function;

    @Nullable
    @JsonProperty(FIELD_VIEW)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String view;

    @JsonCreator
    @ConstructorProperties({FIELD_TYPE, FIELD_DATABASE, FIELD_TABLE, FIELD_FUNCTION, FIELD_VIEW})
    public PermissionResource(
            @JsonProperty(FIELD_TYPE) String type,
            @Nullable @JsonProperty(FIELD_DATABASE) String database,
            @Nullable @JsonProperty(FIELD_TABLE) String table,
            @Nullable @JsonProperty(FIELD_FUNCTION) String function,
            @Nullable @JsonProperty(FIELD_VIEW) String view) {
        this(ResourceType.fromString(type), database, table, function, view);
    }

    public PermissionResource(
            ResourceType type,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        this.type = checkNotNull(type, "resource type cannot be null");
        validate(type, database, table, function, view);
        this.database = blankToNull(database);
        this.table = blankToNull(table);
        this.function = blankToNull(function);
        this.view = blankToNull(view);
    }

    @JsonGetter(FIELD_TYPE)
    public ResourceType getType() {
        return type;
    }

    @Nullable
    @JsonGetter(FIELD_DATABASE)
    public String getDatabase() {
        return database;
    }

    @Nullable
    @JsonGetter(FIELD_TABLE)
    public String getTable() {
        return table;
    }

    @Nullable
    @JsonGetter(FIELD_FUNCTION)
    public String getFunction() {
        return function;
    }

    @Nullable
    @JsonGetter(FIELD_VIEW)
    public String getView() {
        return view;
    }

    /** Validates that this resource can carry a data policy in the current contract. */
    public void validatePolicyAttachment() {
        checkArgument(
                type == ResourceType.TABLE,
                "Policies can currently be attached only to TABLE resources.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PermissionResource)) {
            return false;
        }
        PermissionResource that = (PermissionResource) o;
        return type == that.type
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(function, that.function)
                && Objects.equals(view, that.view);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, database, table, function, view);
    }

    private static void validate(
            ResourceType type,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        switch (type) {
            case CATALOG:
            case CATALOG_ALL:
                checkArgument(
                        isBlank(database) && isBlank(table) && isBlank(function) && isBlank(view),
                        "%s resource cannot contain object identifiers.",
                        type);
                break;
            case DATABASE:
            case DATABASE_ALL:
                checkArgument(!isBlank(database), "database is required for %s resource.", type);
                checkArgument(
                        isBlank(table) && isBlank(function) && isBlank(view),
                        "%s resource cannot contain table, function, or view.",
                        type);
                break;
            case TABLE:
            case COLUMN:
                checkArgument(!isBlank(database), "database is required for %s resource.", type);
                checkArgument(!isBlank(table), "table is required for %s resource.", type);
                checkArgument(
                        isBlank(function) && isBlank(view),
                        "%s resource cannot contain function or view.",
                        type);
                break;
            case FUNCTION:
                checkArgument(!isBlank(database), "database is required for FUNCTION resource.");
                checkArgument(!isBlank(function), "function is required for FUNCTION resource.");
                checkArgument(
                        isBlank(table) && isBlank(view),
                        "FUNCTION resource cannot contain table or view.");
                break;
            case VIEW:
                checkArgument(!isBlank(database), "database is required for VIEW resource.");
                checkArgument(!isBlank(view), "view is required for VIEW resource.");
                checkArgument(
                        isBlank(table) && isBlank(function),
                        "VIEW resource cannot contain table or function.");
                break;
            default:
                throw new IllegalArgumentException("Unsupported resource type " + type);
        }
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }

    @Nullable
    private static String blankToNull(@Nullable String value) {
        return isBlank(value) ? null : value;
    }
}
