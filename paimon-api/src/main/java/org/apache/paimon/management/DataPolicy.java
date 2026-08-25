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
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Named row-filter or column-mask policy attached to one table. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataPolicy {

    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_ROW_FILTER = "rowFilter";
    private static final String FIELD_COLUMN_MASK = "columnMask";
    private static final String FIELD_TO_PRINCIPALS = "toPrincipals";
    private static final String FIELD_EXCEPT_PRINCIPALS = "exceptPrincipals";
    private static final String FIELD_COMMENT = "comment";

    @JsonProperty(FIELD_RESOURCE)
    private final PermissionResource resource;

    @JsonProperty(FIELD_NAME)
    private final String name;

    @Nullable
    @JsonProperty(FIELD_ROW_FILTER)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final RowFilter rowFilter;

    @Nullable
    @JsonProperty(FIELD_COLUMN_MASK)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final ColumnMask columnMask;

    @JsonProperty(FIELD_TO_PRINCIPALS)
    private final List<PrincipalRef> toPrincipals;

    @JsonProperty(FIELD_EXCEPT_PRINCIPALS)
    private final List<PrincipalRef> exceptPrincipals;

    @Nullable
    @JsonProperty(FIELD_COMMENT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String comment;

    @JsonCreator
    @ConstructorProperties({
        FIELD_RESOURCE,
        FIELD_NAME,
        FIELD_ROW_FILTER,
        FIELD_COLUMN_MASK,
        FIELD_TO_PRINCIPALS,
        FIELD_EXCEPT_PRINCIPALS,
        FIELD_COMMENT
    })
    public DataPolicy(
            @JsonProperty(FIELD_RESOURCE) PermissionResource resource,
            @JsonProperty(FIELD_NAME) String name,
            @Nullable @JsonProperty(FIELD_ROW_FILTER) RowFilter rowFilter,
            @Nullable @JsonProperty(FIELD_COLUMN_MASK) ColumnMask columnMask,
            @JsonProperty(FIELD_TO_PRINCIPALS) List<PrincipalRef> toPrincipals,
            @Nullable @JsonProperty(FIELD_EXCEPT_PRINCIPALS) List<PrincipalRef> exceptPrincipals,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        resource.validatePolicyAttachment();
        checkArgument(!isBlank(name), "policy name cannot be empty.");
        this.name = name;
        checkArgument(
                (rowFilter == null) != (columnMask == null),
                "A policy must contain exactly one of rowFilter and columnMask.");
        this.rowFilter = rowFilter;
        this.columnMask = columnMask;
        checkArgument(
                toPrincipals != null && !toPrincipals.isEmpty(),
                "toPrincipals must contain at least one principal.");
        checkArgument(
                toPrincipals.stream().noneMatch(Objects::isNull),
                "toPrincipals cannot contain null principals.");
        checkArgument(
                new HashSet<>(toPrincipals).size() == toPrincipals.size(),
                "toPrincipals cannot contain duplicate principals.");
        this.toPrincipals = immutable(toPrincipals);
        List<PrincipalRef> exclusions = immutable(exceptPrincipals);
        checkArgument(
                exclusions.stream().noneMatch(Objects::isNull),
                "exceptPrincipals cannot contain null principals.");
        checkArgument(
                new HashSet<>(exclusions).size() == exclusions.size(),
                "exceptPrincipals cannot contain duplicate principals.");
        this.exceptPrincipals = exclusions;
        this.comment = isBlank(comment) ? null : comment;
    }

    public static DataPolicy rowFilter(
            PermissionResource resource,
            String name,
            RowFilter rowFilter,
            List<PrincipalRef> toPrincipals,
            @Nullable List<PrincipalRef> exceptPrincipals,
            @Nullable String comment) {
        return new DataPolicy(
                resource, name, rowFilter, null, toPrincipals, exceptPrincipals, comment);
    }

    public static DataPolicy columnMask(
            PermissionResource resource,
            String name,
            ColumnMask columnMask,
            List<PrincipalRef> toPrincipals,
            @Nullable List<PrincipalRef> exceptPrincipals,
            @Nullable String comment) {
        return new DataPolicy(
                resource, name, null, columnMask, toPrincipals, exceptPrincipals, comment);
    }

    @JsonGetter(FIELD_RESOURCE)
    public PermissionResource getResource() {
        return resource;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
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

    @JsonGetter(FIELD_TO_PRINCIPALS)
    public List<PrincipalRef> getToPrincipals() {
        return toPrincipals;
    }

    @JsonGetter(FIELD_EXCEPT_PRINCIPALS)
    public List<PrincipalRef> getExceptPrincipals() {
        return exceptPrincipals;
    }

    @Nullable
    @JsonGetter(FIELD_COMMENT)
    public String getComment() {
        return comment;
    }

    private static <T> List<T> immutable(@Nullable List<T> values) {
        return values == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(values));
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
