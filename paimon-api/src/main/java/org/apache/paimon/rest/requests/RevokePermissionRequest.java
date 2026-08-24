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

import org.apache.paimon.management.ColumnSelection;
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

/** Flat request for revoking a permission by its resource identity. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RevokePermissionRequest implements RESTRequest {

    private static final String FIELD_RESOURCE_TYPE = "resourceType";
    private static final String FIELD_CATALOG = "catalog";
    private static final String FIELD_DATABASE = "database";
    private static final String FIELD_TABLE = "table";
    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_VIEW = "view";
    private static final String FIELD_COLUMNS = "columns";
    private static final String FIELD_ACCESS = "access";
    private static final String FIELD_PRINCIPAL = "principal";

    private final ResourceType resourceType;
    @Nullable private final String catalog;
    @Nullable private final String database;
    @Nullable private final String table;
    @Nullable private final String function;
    @Nullable private final String view;
    @Nullable private final ColumnSelection columns;
    private final String access;
    private final String principal;

    public RevokePermissionRequest(Permission permission) {
        this(
                permission.getResourceType(),
                permission.getCatalog(),
                permission.getDatabase(),
                permission.getTable(),
                permission.getFunction(),
                permission.getView(),
                permission.getColumns(),
                permission.getAccess(),
                permission.getPrincipal());
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
        FIELD_ACCESS,
        FIELD_PRINCIPAL
    })
    public RevokePermissionRequest(
            @JsonProperty(FIELD_RESOURCE_TYPE) ResourceType resourceType,
            @Nullable @JsonProperty(FIELD_CATALOG) String catalog,
            @Nullable @JsonProperty(FIELD_DATABASE) String database,
            @Nullable @JsonProperty(FIELD_TABLE) String table,
            @Nullable @JsonProperty(FIELD_FUNCTION) String function,
            @Nullable @JsonProperty(FIELD_VIEW) String view,
            @Nullable @JsonProperty(FIELD_COLUMNS) ColumnSelection columns,
            @JsonProperty(FIELD_ACCESS) String access,
            @JsonProperty(FIELD_PRINCIPAL) String principal) {
        this.resourceType = resourceType;
        this.catalog = catalog;
        this.database = database;
        this.table = table;
        this.function = function;
        this.view = view;
        this.columns = columns;
        this.access = access;
        this.principal = principal;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public ResourceType getResourceType() {
        return resourceType;
    }

    @Nullable
    @JsonGetter(FIELD_CATALOG)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getCatalog() {
        return catalog;
    }

    @Nullable
    @JsonGetter(FIELD_DATABASE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getDatabase() {
        return database;
    }

    @Nullable
    @JsonGetter(FIELD_TABLE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getTable() {
        return table;
    }

    @Nullable
    @JsonGetter(FIELD_FUNCTION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getFunction() {
        return function;
    }

    @Nullable
    @JsonGetter(FIELD_VIEW)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getView() {
        return view;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMNS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ColumnSelection getColumns() {
        return columns;
    }

    @JsonGetter(FIELD_ACCESS)
    public String getAccess() {
        return access;
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return principal;
    }
}
