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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PrincipalRef;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.List;

/** Create or replace payload for a policy whose table is identified by the request path. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PolicyRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_ROW_FILTER = "rowFilter";
    private static final String FIELD_COLUMN_MASK = "columnMask";
    private static final String FIELD_TO_PRINCIPALS = "toPrincipals";
    private static final String FIELD_EXCEPT_PRINCIPALS = "exceptPrincipals";
    private static final String FIELD_COMMENT = "comment";

    private final String name;
    @Nullable private final RowFilter rowFilter;
    @Nullable private final ColumnMask columnMask;
    private final List<PrincipalRef> toPrincipals;
    private final List<PrincipalRef> exceptPrincipals;
    @Nullable private final String comment;

    public PolicyRequest(DataPolicy policy) {
        this(
                policy.getName(),
                policy.getRowFilter(),
                policy.getColumnMask(),
                policy.getToPrincipals(),
                policy.getExceptPrincipals(),
                policy.getComment());
    }

    @JsonCreator
    @ConstructorProperties({
        FIELD_NAME,
        FIELD_ROW_FILTER,
        FIELD_COLUMN_MASK,
        FIELD_TO_PRINCIPALS,
        FIELD_EXCEPT_PRINCIPALS,
        FIELD_COMMENT
    })
    public PolicyRequest(
            @JsonProperty(FIELD_NAME) String name,
            @Nullable @JsonProperty(FIELD_ROW_FILTER) RowFilter rowFilter,
            @Nullable @JsonProperty(FIELD_COLUMN_MASK) ColumnMask columnMask,
            @JsonProperty(FIELD_TO_PRINCIPALS) List<PrincipalRef> toPrincipals,
            @Nullable @JsonProperty(FIELD_EXCEPT_PRINCIPALS) List<PrincipalRef> exceptPrincipals,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment) {
        this.name = name;
        this.rowFilter = rowFilter;
        this.columnMask = columnMask;
        this.toPrincipals = toPrincipals;
        this.exceptPrincipals = exceptPrincipals;
        this.comment = comment;
    }

    public DataPolicy policy(PermissionResource resource) {
        return new DataPolicy(
                resource, name, rowFilter, columnMask, toPrincipals, exceptPrincipals, comment);
    }

    /** Creating a named resource cannot be replayed after an ambiguous server response. */
    @JsonIgnore
    @Override
    public boolean isRetrySafe() {
        return false;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @Nullable
    @JsonGetter(FIELD_ROW_FILTER)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public RowFilter getRowFilter() {
        return rowFilter;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN_MASK)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ColumnMask getColumnMask() {
        return columnMask;
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
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getComment() {
        return comment;
    }
}
