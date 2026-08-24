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

package org.apache.paimon.rest.requests;

import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.ColumnSelection;
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.Map;

/** Flat request for granting a permission. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GrantPermissionRequest implements RESTRequest {

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

    private final Permission permission;

    public GrantPermissionRequest(Permission permission) {
        this.permission = permission;
    }

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
    public GrantPermissionRequest(
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
        this.permission =
                new Permission(
                        resourceType,
                        catalog,
                        database,
                        table,
                        function,
                        view,
                        columns,
                        rowFilter,
                        columnMasking,
                        access,
                        principal,
                        expireTime);
    }

    public Permission permission() {
        return permission;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public ResourceType getResourceType() {
        return permission.getResourceType();
    }

    @Nullable
    @JsonGetter(FIELD_CATALOG)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getCatalog() {
        return permission.getCatalog();
    }

    @Nullable
    @JsonGetter(FIELD_DATABASE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDatabase() {
        return permission.getDatabase();
    }

    @Nullable
    @JsonGetter(FIELD_TABLE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getTable() {
        return permission.getTable();
    }

    @Nullable
    @JsonGetter(FIELD_FUNCTION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getFunction() {
        return permission.getFunction();
    }

    @Nullable
    @JsonGetter(FIELD_VIEW)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getView() {
        return permission.getView();
    }

    @Nullable
    @JsonGetter(FIELD_COLUMNS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ColumnSelection getColumns() {
        return permission.getColumns();
    }

    @Nullable
    @JsonGetter(FIELD_ROW_FILTER)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public RowFilter getRowFilter() {
        return permission.getRowFilter();
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_MASKING)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Map<String, ColumnMask> getColumnMasking() {
        return permission.getColumnMasking();
    }

    @JsonGetter(FIELD_ACCESS)
    public String getAccess() {
        return permission.getAccess();
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return permission.getPrincipal();
    }

    @Nullable
    @JsonGetter(FIELD_EXPIRE_TIME)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getExpireTime() {
        return permission.getExpireTime();
    }
}
