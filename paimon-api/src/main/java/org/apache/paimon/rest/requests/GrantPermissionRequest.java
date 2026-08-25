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
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

/** Request for granting or replacing a permission assignment. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class GrantPermissionRequest implements RESTRequest {

    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_ACCESS = "access";
    private static final String FIELD_PRINCIPAL = "principal";
    private static final String FIELD_COLUMNS = "columns";
    private static final String FIELD_EXPIRE_TIME = "expireTime";

    private final PermissionAssignment assignment;

    public GrantPermissionRequest(PermissionAssignment assignment) {
        this.assignment = assignment;
    }

    @JsonCreator
    @ConstructorProperties({
        FIELD_RESOURCE,
        FIELD_ACCESS,
        FIELD_PRINCIPAL,
        FIELD_COLUMNS,
        FIELD_EXPIRE_TIME
    })
    public GrantPermissionRequest(
            @JsonProperty(FIELD_RESOURCE) PermissionResource resource,
            @JsonProperty(FIELD_ACCESS) String access,
            @JsonProperty(FIELD_PRINCIPAL) String principal,
            @Nullable @JsonProperty(FIELD_COLUMNS) PermissionColumns columns,
            @Nullable @JsonProperty(FIELD_EXPIRE_TIME) String expireTime) {
        this.assignment =
                new PermissionAssignment(resource, access, principal, columns, expireTime);
    }

    public PermissionAssignment assignment() {
        return assignment;
    }

    @JsonGetter(FIELD_RESOURCE)
    public PermissionResource getResource() {
        return assignment.getResource();
    }

    @JsonGetter(FIELD_ACCESS)
    public String getAccess() {
        return assignment.getAccess();
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public String getPrincipal() {
        return assignment.getPrincipal();
    }

    @Nullable
    @JsonGetter(FIELD_COLUMNS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public PermissionColumns getColumns() {
        return assignment.getColumns();
    }

    @Nullable
    @JsonGetter(FIELD_EXPIRE_TIME)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getExpireTime() {
        return assignment.getExpireTime();
    }
}
