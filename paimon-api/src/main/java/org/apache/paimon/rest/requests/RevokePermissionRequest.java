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
import org.apache.paimon.management.PermissionAccess;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;

/** Idempotent request for revoking a permission assignment by identity. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class RevokePermissionRequest implements RESTRequest {

    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_ACCESS = "access";
    private static final String FIELD_PRINCIPAL = "principal";

    private final PermissionResource resource;
    private final String access;
    private final String principal;

    @JsonCreator
    @ConstructorProperties({FIELD_RESOURCE, FIELD_ACCESS, FIELD_PRINCIPAL})
    public RevokePermissionRequest(
            @JsonProperty(FIELD_RESOURCE) PermissionResource resource,
            @JsonProperty(FIELD_ACCESS) String access,
            @JsonProperty(FIELD_PRINCIPAL) String principal) {
        this.resource = resource;
        this.access = PermissionAccess.canonicalize(resource, access);
        this.principal =
                org.apache.paimon.management.PermissionAssignment.validatePrincipal(principal);
    }

    @JsonGetter(FIELD_RESOURCE)
    public PermissionResource getResource() {
        return resource;
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
