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

/**
 * Direct permission assignment or a resource-inherited view of one.
 *
 * <p>{@code expireTime}, when present, is an exclusive authorization upper bound evaluated against
 * the server clock. At or after that instant, the assignment must not authorize access or produce
 * an effective inherited view, although a direct expired record may remain listable until cleanup.
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PermissionAssignment {

    /** Maximum principal identifier length in the portable REST management contract. */
    public static final int MAX_PRINCIPAL_LENGTH = 128;

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
    private final String principal;

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
            @JsonProperty(FIELD_PRINCIPAL) String principal,
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
            String principal,
            @Nullable String expireTime,
            @Nullable PermissionResource inheritedFrom) {
        this.resource = checkNotNull(resource, "resource cannot be null");
        this.scope = checkNotNull(scope, "scope cannot be null");
        this.access = PermissionAccess.canonicalize(resource, scope, access);
        this.principal = validatePrincipal(principal);
        if (expireTime != null) {
            try {
                Instant instant = Instant.parse(expireTime);
                checkArgument(
                        instant.getNano() % 1_000_000 == 0,
                        "expireTime must have at most millisecond precision.");
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
    public String getPrincipal() {
        return principal;
    }

    static String validatePrincipal(String principal) {
        checkArgument(
                principal != null && !principal.trim().isEmpty(), "principal cannot be empty.");
        checkArgument(
                principal.length() <= MAX_PRINCIPAL_LENGTH,
                "principal must contain at most %s characters.",
                MAX_PRINCIPAL_LENGTH);
        return principal;
    }

    /** Returns the exclusive authorization upper bound, or null for no expiry. */
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
