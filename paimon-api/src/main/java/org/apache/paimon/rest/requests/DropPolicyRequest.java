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
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Request for dropping one principal's row filter or column mask. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class DropPolicyRequest implements RESTRequest {

    private static final String FIELD_TYPE = "type";
    private static final String FIELD_PRINCIPAL = "principal";
    private static final String FIELD_COLUMN = "column";

    private final PolicyType type;
    private final String principal;
    @Nullable private final String column;

    @JsonCreator
    @ConstructorProperties({FIELD_TYPE, FIELD_PRINCIPAL, FIELD_COLUMN})
    public DropPolicyRequest(
            @JsonProperty(FIELD_TYPE) PolicyType type,
            @JsonProperty(FIELD_PRINCIPAL) String principal,
            @Nullable @JsonProperty(FIELD_COLUMN) String column) {
        this.type = checkNotNull(type, "policy type cannot be null");
        this.principal = validatePrincipal(principal);
        if (type == PolicyType.ROW_FILTER) {
            checkArgument(isBlank(column), "ROW_FILTER identity cannot contain a column.");
            this.column = null;
        } else {
            checkArgument(!isBlank(column), "column is required for COLUMN_MASKING identity.");
            this.column = column;
        }
    }

    @JsonGetter(FIELD_TYPE)
    public PolicyType getType() {
        return type;
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return principal;
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getColumn() {
        return column;
    }

    private static String validatePrincipal(String principal) {
        checkArgument(
                principal != null && !principal.trim().isEmpty(), "principal cannot be empty.");
        checkArgument(
                principal.length() <= PermissionAssignment.MAX_PRINCIPAL_LENGTH,
                "principal must contain at most %s characters.",
                PermissionAssignment.MAX_PRINCIPAL_LENGTH);
        return principal;
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
