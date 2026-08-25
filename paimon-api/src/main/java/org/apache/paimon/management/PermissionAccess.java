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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Built-in access names and validation for permission assignments. */
@Experimental
public final class PermissionAccess {

    public static final String USE_CATALOG = "USE_CATALOG";
    public static final String CREATE_DATABASE = "CREATE_DATABASE";
    public static final String USE_DATABASE = "USE_DATABASE";
    public static final String CREATE_TABLE = "CREATE_TABLE";
    public static final String CREATE_VIEW = "CREATE_VIEW";
    public static final String CREATE_FUNCTION = "CREATE_FUNCTION";
    public static final String SELECT = "SELECT";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String ALTER = "ALTER";
    public static final String DROP = "DROP";
    public static final String EXECUTE = "EXECUTE";
    public static final String MANAGE_PERMISSIONS = "MANAGE_PERMISSIONS";

    private static final Pattern EXTENSION_ACCESS =
            Pattern.compile("[A-Z0-9][A-Z0-9._-]*/[A-Z0-9][A-Z0-9._-]*");
    private static final Map<ResourceType, Set<String>> BUILT_INS = builtIns();

    private PermissionAccess() {}

    public static String canonicalize(String access) {
        checkArgument(access != null && !access.trim().isEmpty(), "access cannot be empty.");
        String canonical = access.toUpperCase(Locale.ROOT);
        if (EXTENSION_ACCESS.matcher(canonical).matches()
                || BUILT_INS.values().stream().anyMatch(values -> values.contains(canonical))) {
            return canonical;
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unknown access '%s'. Use a built-in access or a namespaced extension such as vendor.example/SOME_ACCESS.",
                        canonical));
    }

    public static String canonicalize(
            PermissionResource resource, PermissionScope scope, String access) {
        String canonical = canonicalize(access);
        if (EXTENSION_ACCESS.matcher(canonical).matches()) {
            return canonical;
        }

        checkArgument(
                appliesTo(resource.getType(), scope, canonical),
                "Access '%s' is not valid for %s with %s scope. Use a built-in access or a namespaced extension such as vendor.example/SOME_ACCESS.",
                canonical,
                resource.getType(),
                scope);
        return canonical;
    }

    /** Validates an access filter when the query may return either scope. */
    public static String canonicalize(PermissionResource resource, String access) {
        String canonical = canonicalize(access);
        if (EXTENSION_ACCESS.matcher(canonical).matches()) {
            return canonical;
        }
        boolean applicable = appliesTo(resource.getType(), PermissionScope.SELF, canonical);
        if (!applicable
                && (resource.getType() == ResourceType.CATALOG
                        || resource.getType() == ResourceType.DATABASE)) {
            applicable = appliesTo(resource.getType(), PermissionScope.DESCENDANTS, canonical);
        }
        checkArgument(
                applicable,
                "Access '%s' is not valid for assignments on %s.",
                canonical,
                resource.getType());
        return canonical;
    }

    public static Set<String> builtIns(ResourceType type) {
        return BUILT_INS.get(type);
    }

    private static boolean appliesTo(ResourceType type, PermissionScope scope, String access) {
        if (scope == PermissionScope.SELF) {
            return BUILT_INS.get(type).contains(access);
        }
        checkArgument(
                type == ResourceType.CATALOG || type == ResourceType.DATABASE,
                "DESCENDANTS scope is supported only for CATALOG and DATABASE resources.");
        if (type == ResourceType.CATALOG && BUILT_INS.get(ResourceType.DATABASE).contains(access)) {
            return true;
        }
        return BUILT_INS.get(ResourceType.TABLE).contains(access)
                || BUILT_INS.get(ResourceType.VIEW).contains(access)
                || BUILT_INS.get(ResourceType.FUNCTION).contains(access);
    }

    private static Map<ResourceType, Set<String>> builtIns() {
        Map<ResourceType, Set<String>> accesses = new EnumMap<>(ResourceType.class);
        accesses.put(
                ResourceType.CATALOG, values(USE_CATALOG, CREATE_DATABASE, MANAGE_PERMISSIONS));
        accesses.put(
                ResourceType.DATABASE,
                values(
                        USE_DATABASE,
                        CREATE_TABLE,
                        CREATE_VIEW,
                        CREATE_FUNCTION,
                        ALTER,
                        DROP,
                        MANAGE_PERMISSIONS));
        accesses.put(
                ResourceType.TABLE,
                values(SELECT, INSERT, UPDATE, DELETE, ALTER, DROP, MANAGE_PERMISSIONS));
        accesses.put(ResourceType.VIEW, values(SELECT, ALTER, DROP, MANAGE_PERMISSIONS));
        accesses.put(ResourceType.FUNCTION, values(EXECUTE, ALTER, DROP, MANAGE_PERMISSIONS));
        return Collections.unmodifiableMap(accesses);
    }

    private static Set<String> values(String... accesses) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(accesses)));
    }
}
