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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionIdentity;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PermissionScope;
import org.apache.paimon.management.PolicyManagement;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;

import org.apache.spark.sql.connector.catalog.TableCatalog;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Locale;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared REST catalog lookup and argument validation for management procedures. */
abstract class BasePermissionProcedure extends BaseProcedure {

    protected BasePermissionProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    protected PermissionManagement permissionManagement() {
        return restCatalog().permissionManagement();
    }

    protected PolicyManagement policyManagement() {
        return restCatalog().policyManagement();
    }

    private RESTCatalog restCatalog() {
        checkArgument(
                tableCatalog() instanceof WithPaimonCatalog,
                "Catalog '%s' is not a Paimon catalog.",
                tableCatalog().name());
        Catalog root =
                DelegateCatalog.rootCatalog(((WithPaimonCatalog) tableCatalog()).paimonCatalog());
        checkArgument(
                root instanceof RESTCatalog,
                "Catalog '%s' does not support permission or policy management.",
                tableCatalog().name());
        return (RESTCatalog) root;
    }

    protected static PermissionAssignment assignment(
            ResourceType resourceType,
            String access,
            String principal,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable String scope,
            @Nullable String expireTime) {
        return new PermissionAssignment(
                resource(resourceType, database, table, function, view),
                scope(scope),
                access,
                principal,
                emptyToNull(expireTime),
                null);
    }

    protected static PermissionIdentity identity(
            ResourceType resourceType,
            String access,
            String principal,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view,
            @Nullable String scope) {
        return new PermissionIdentity(
                resource(resourceType, database, table, function, view),
                scope(scope),
                access,
                principal);
    }

    protected static PermissionResource resource(
            ResourceType resourceType,
            @Nullable String database,
            @Nullable String table,
            @Nullable String function,
            @Nullable String view) {
        return new PermissionResource(
                resourceType,
                emptyToNull(database),
                emptyToNull(table),
                emptyToNull(function),
                emptyToNull(view));
    }

    protected static PermissionScope scope(@Nullable String value) {
        return isBlank(value)
                ? PermissionScope.SELF
                : enumValue(value, PermissionScope.class, "scope");
    }

    protected static <E extends Enum<E>> E enumValue(
            String value, Class<E> enumClass, String argument) {
        checkArgument(!isBlank(value), "%s cannot be empty.", argument);
        try {
            return Enum.valueOf(enumClass, value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid %s '%s'. Expected one of %s.",
                            argument, value, Arrays.toString(enumClass.getEnumConstants())),
                    e);
        }
    }

    @Nullable
    protected static <E extends Enum<E>> E optionalEnum(
            @Nullable String value, Class<E> enumClass, String argument) {
        return isBlank(value) ? null : enumValue(value, enumClass, argument);
    }

    @Nullable
    protected static String emptyToNull(@Nullable String value) {
        return isBlank(value) ? null : value;
    }

    protected static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
