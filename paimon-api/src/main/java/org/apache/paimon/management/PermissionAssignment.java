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
import java.time.Instant;
import java.time.format.DateTimeParseException;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Direct permission assignment or a resource-inherited view of one. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PermissionAssignment {

    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_SCOPE = "scope";
    private static final String FIELD_ACCESS = "access";
    private static final String FIELD_PRINCIPAL = "principal";
    private static final String FIELD_EXPIRE_TIME = "expireTime";
    private static final String FIELD_INHERITED_FROM = "inheritedFrom";

    @JsonProperty(FIELD_RESOURCE)
    private final PermissionResource resource;

    @JsonProperty(FIELD_SCOPE)
    private final PermissionScope scope;

    @JsonProperty(FIELD_ACCESS)
    private final String access;

    @JsonProperty(FIELD_PRINCIPAL)
    private final PrincipalRef principal;

    @Nullable
    @JsonProperty(FIELD_EXPIRE_TIME)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String expireTime;

    @Nullable
    @JsonProperty(FIELD_INHERITED_FROM)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final PermissionResource inheritedFrom;

    @JsonCreator
    @ConstructorProperties({
        FIELD_RESOURCE,
        FIELD_SCOPE,
        FIELD_ACCESS,
        FIELD_PRINCIPAL,
        FIELD_EXPIRE_TIME,
        FIELD_INHERITED_FROM
    })
    public PermissionAssignment(
            @JsonProperty(FIELD_RESOURCE) PermissionResource resource,
            @Nullable @JsonProperty(FIELD_SCOPE) String scope,
            @JsonProperty(FIELD_ACCESS) String access,
            @JsonProperty(FIELD_PRINCIPAL) PrincipalRef principal,
            @Nullable @JsonProperty(FIELD_EXPIRE_TIME) String expireTime,
            @Nullable @JsonProperty(FIELD_INHERITED_FROM) PermissionResource inheritedFrom) {
        this(
                resource,
                scope == null ? PermissionScope.SELF : PermissionScope.fromString(scope),
                access,
                principal,
                expireTime,
                inheritedFrom);
    }

    public PermissionAssignment(
            PermissionResource resource,
            PermissionScope scope,
            String access,
            PrincipalRef principal,
            @Nullable String expireTime,
            @Nullable PermissionResource inheritedFrom) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        this.scope = checkNotNull(scope, "scope cannot be null");
        this.access = PermissionAccess.canonicalize(resource, scope, access);
        this.principal = checkNotNull(principal, "principal cannot be null");
        if (expireTime != null) {
            try {
                Instant.parse(expireTime);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "expireTime must be an ISO-8601 UTC instant.", e);
            }
        }
        checkArgument(
                inheritedFrom == null || scope == PermissionScope.SELF,
                "An inherited assignment view must use SELF scope.");
        this.expireTime = expireTime;
        this.inheritedFrom = inheritedFrom;
    }

    @JsonGetter(FIELD_RESOURCE)
    public PermissionResource getResource() {
        return resource;
    }

    @JsonGetter(FIELD_SCOPE)
    public PermissionScope getScope() {
        return scope;
    }

    @JsonGetter(FIELD_ACCESS)
    public String getAccess() {
        return access;
    }

    @JsonGetter(FIELD_PRINCIPAL)
    public PrincipalRef getPrincipal() {
        return principal;
    }

    @Nullable
    @JsonGetter(FIELD_EXPIRE_TIME)
    public String getExpireTime() {
        return expireTime;
    }

    @Nullable
    @JsonGetter(FIELD_INHERITED_FROM)
    public PermissionResource getInheritedFrom() {
        return inheritedFrom;
    }
}
