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

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Built-in access names and validation for permission assignments. */
@Experimental
public final class PermissionAccess {

    /** Maximum wire length supported by the portable permission storage contract. */
    public static final int MAX_LENGTH = 32;

    public static final String ALL = "ALL";
    public static final String CREATEDATABASE = "CREATEDATABASE";
    public static final String DESCRIBE = "DESCRIBE";
    public static final String ALTER = "ALTER";
    public static final String DROP = "DROP";
    public static final String CREATETABLE = "CREATETABLE";
    public static final String CREATEFUNCTION = "CREATEFUNCTION";
    public static final String CREATEVIEW = "CREATEVIEW";
    public static final String LIST = "LIST";
    public static final String SELECT = "SELECT";
    public static final String UPDATE = "UPDATE";
    public static final String GRANT = "GRANT";

    private static final Map<ResourceType, Set<String>> BUILT_INS = builtIns();

    private PermissionAccess() {}

    public static String canonicalize(String access) {
        checkArgument(access != null && !access.trim().isEmpty(), "access cannot be empty.");
        checkArgument(
                access.length() <= MAX_LENGTH,
                "access must contain at most %s characters.",
                MAX_LENGTH);
        String canonical = access.toUpperCase(Locale.ROOT);
        checkArgument(
                canonical.length() <= MAX_LENGTH,
                "access must contain at most %s characters after canonicalization.",
                MAX_LENGTH);
        if (BUILT_INS.values().stream().anyMatch(values -> values.contains(canonical))) {
            return canonical;
        }
        throw new IllegalArgumentException(String.format("Unknown access '%s'.", canonical));
    }

    public static String canonicalize(PermissionResource resource, String access) {
        checkNotNull(resource, "resource cannot be null");
        String canonical = canonicalize(access);
        checkArgument(
                BUILT_INS.get(resource.getType()).contains(canonical),
                "Access '%s' is not valid for %s.",
                canonical,
                resource.getType());
        return canonical;
    }

    public static Set<String> builtIns(ResourceType type) {
        return BUILT_INS.get(checkNotNull(type, "resource type cannot be null"));
    }

    private static Map<ResourceType, Set<String>> builtIns() {
        Map<ResourceType, Set<String>> accesses = new EnumMap<>(ResourceType.class);
        accesses.put(ResourceType.CATALOG, values(ALL, ALTER, DROP, GRANT, CREATEDATABASE));
        accesses.put(
                ResourceType.DATABASE,
                values(
                        ALL,
                        DESCRIBE,
                        ALTER,
                        DROP,
                        GRANT,
                        CREATETABLE,
                        CREATEVIEW,
                        CREATEFUNCTION,
                        LIST));
        accesses.put(ResourceType.TABLE, values(ALL, SELECT, UPDATE, ALTER, DROP, GRANT));
        accesses.put(ResourceType.COLUMN, values(SELECT));
        accesses.put(ResourceType.VIEW, values(ALL, SELECT, ALTER, DROP, GRANT));
        accesses.put(ResourceType.FUNCTION, values(ALL, SELECT, ALTER, DROP, GRANT));
        return Collections.unmodifiableMap(accesses);
    }

    private static Set<String> values(String... accesses) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(accesses)));
    }
}
