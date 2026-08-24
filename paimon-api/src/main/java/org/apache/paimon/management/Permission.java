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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.Map;

/** Permission grant on a catalog resource. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Permission {

    private static final String FIELD_RESOURCE_TYPE = "resourceType";
    private static final String FIELD_CATALOG = "catalog";
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_VIEW = "view";
    private static final String FIELD_COLUMNS = "columns";
    private static final String FIELD_ROW_FILTER = "rowFilter";
    private static final String FIELD_COLUMN_MASKING = "columnMasking";
    private static final String FIELD_ACCESS = "access";
    private static final String FIELD_PRINCIPAL = "principal";
    private static final String FIELD_EXPIRE_TIME = "expireTime";

    @JsonProperty(FIELD_RESOURCE_TYPE)
    private final ResourceType resourceType;

    @Nullable
    @JsonProperty(FIELD_CATALOG)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String catalog;

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

    @Nullable
    @JsonProperty(FIELD_COLUMNS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final ColumnSelection columns;

    @Nullable
    @JsonProperty(FIELD_ROW_FILTER)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final RowFilter rowFilter;

    @Nullable
    @JsonProperty(FIELD_COLUMN_MASKING)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Map<String, ColumnMask> columnMasking;

    @JsonProperty(FIELD_ACCESS)
    private final String access;

    @JsonProperty(FIELD_PRINCIPAL)
    private final String principal;

    @Nullable
    @JsonProperty(FIELD_EXPIRE_TIME)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String expireTime;

    @JsonCreator
    @ConstructorProperties({
        FIELD_RESOURCE_TYPE,
        FIELD_CATALOG,
        FIELD_DATABASE,
        FIELD_TABLE,
        FIELD_FUNCTION,
        FIELD_VIEW,
        FIELD_COLUMNS,
        FIELD_ROW_FILTER,
        FIELD_COLUMN_MASKING,
        FIELD_ACCESS,
        FIELD_PRINCIPAL,
        FIELD_EXPIRE_TIME
    })
    public Permission(
            @JsonProperty(FIELD_RESOURCE_TYPE) ResourceType resourceType,
            @Nullable @JsonProperty(FIELD_CATALOG) String catalog,
            @Nullable @JsonProperty(FIELD_DATABASE) String database,
            @Nullable @JsonProperty(FIELD_TABLE) String table,
            @Nullable @JsonProperty(FIELD_FUNCTION) String function,
            @Nullable @JsonProperty(FIELD_VIEW) String view,
            @Nullable @JsonProperty(FIELD_COLUMNS) ColumnSelection columns,
            @Nullable @JsonProperty(FIELD_ROW_FILTER) RowFilter rowFilter,
            @Nullable @JsonProperty(FIELD_COLUMN_MASKING) Map<String, ColumnMask> columnMasking,
            @JsonProperty(FIELD_ACCESS) String access,
            @JsonProperty(FIELD_PRINCIPAL) String principal,
            @Nullable @JsonProperty(FIELD_EXPIRE_TIME) String expireTime) {
        this.resourceType = resourceType;
        this.catalog = catalog;
        this.database = database;
        this.table = table;
        this.function = function;
        this.view = view;
        this.columns = columns;
        this.rowFilter = rowFilter;
        this.columnMasking = columnMasking;
        this.access = access;
        this.principal = principal;
        this.expireTime = expireTime;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public ResourceType getResourceType() {
        return resourceType;
    }

    @Nullable
    @JsonGetter(FIELD_CATALOG)
    public String getCatalog() {
        return catalog;
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

    @Nullable
    @JsonGetter(FIELD_COLUMNS)
    public ColumnSelection getColumns() {
        return columns;
    }

    @Nullable
    @JsonGetter(FIELD_ROW_FILTER)
    public RowFilter getRowFilter() {
        return rowFilter;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_MASKING)
    public Map<String, ColumnMask> getColumnMasking() {
        return columnMasking;
    }

    @JsonGetter(FIELD_ACCESS)
    public String getAccess() {
        return access;
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return principal;
    }

    @Nullable
    @JsonGetter(FIELD_EXPIRE_TIME)
    public String getExpireTime() {
        return expireTime;
    }
}
