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

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Principal-scoped row-filter or column-mask policy attached to one table.
 *
 * <p>A principal has at most one row filter per table and at most one mask per table column. When
 * policies are enforced, applicable row filters are combined with logical AND and multiple
 * effective masks for one column fail closed. Invalid predicates or transforms also fail closed.
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPolicy {

    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_ROW_FILTER = "rowFilter";
    private static final String FIELD_COLUMN_MASK = "columnMask";
    private static final String FIELD_PRINCIPAL = "principal";

    @JsonProperty(FIELD_RESOURCE)
    private final PermissionResource resource;

    @Nullable
    @JsonProperty(FIELD_ROW_FILTER)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final RowFilter rowFilter;

    @Nullable
    @JsonProperty(FIELD_COLUMN_MASK)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final ColumnMask columnMask;

    @JsonProperty(FIELD_PRINCIPAL)
    private final String principal;

    @JsonCreator
    @ConstructorProperties({FIELD_RESOURCE, FIELD_ROW_FILTER, FIELD_COLUMN_MASK, FIELD_PRINCIPAL})
    public DataPolicy(
            @JsonProperty(FIELD_RESOURCE) PermissionResource resource,
            @Nullable @JsonProperty(FIELD_ROW_FILTER) RowFilter rowFilter,
            @Nullable @JsonProperty(FIELD_COLUMN_MASK) ColumnMask columnMask,
            @JsonProperty(FIELD_PRINCIPAL) String principal) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        checkArgument(
                (rowFilter == null) != (columnMask == null),
                "A policy must contain exactly one of rowFilter and columnMask.");
        this.rowFilter = rowFilter;
        this.columnMask = columnMask;
        this.principal = PermissionAssignment.validatePrincipal(principal);
    }

    public static DataPolicy rowFilter(
            PermissionResource resource, RowFilter rowFilter, String principal) {
        return new DataPolicy(resource, rowFilter, null, principal);
    }

    public static DataPolicy columnMask(
            PermissionResource resource, ColumnMask columnMask, String principal) {
        return new DataPolicy(resource, null, columnMask, principal);
    }

    @JsonGetter(FIELD_RESOURCE)
    public PermissionResource getResource() {
        return resource;
    }

    @Nullable
    @JsonGetter(FIELD_ROW_FILTER)
    public RowFilter getRowFilter() {
        return rowFilter;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_MASK)
    public ColumnMask getColumnMask() {
        return columnMask;
    }

    public PolicyType type() {
        return rowFilter == null ? PolicyType.COLUMN_MASKING : PolicyType.ROW_FILTER;
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return principal;
    }
}
