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

package org.apache.paimon.rest;

import org.apache.paimon.management.PermissionAccess;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionIdentity;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PermissionScope;
import org.apache.paimon.management.ResourceType;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Atomic permission assignment store for the REST catalog test server. */
final class RESTPermissionStore {

    private final Map<PermissionIdentity, PermissionAssignment> assignments =
            new ConcurrentHashMap<>();

    void put(PermissionAssignment assignment) {
        assignments.put(PermissionIdentity.fromAssignment(assignment), assignment);
    }

    void remove(PermissionIdentity identity) {
        assignments.remove(identity);
    }

    List<PermissionAssignment> list(
            PermissionResource target, Map<String, String> parameters, boolean includeInherited) {
        return assignments.values().stream()
                .filter(assignment -> matchesSource(assignment, parameters, includeInherited))
                .filter(assignment -> !includeInherited || isEffectiveOn(assignment, target))
                .map(
                        assignment ->
                                includeInherited
                                        ? effectiveAssignment(assignment, target)
                                        : assignment)
                .filter(assignment -> matches(parameters, "scope", assignment.getScope().name()))
                .sorted(Comparator.comparing(RESTPermissionStore::sortKey))
                .collect(Collectors.toList());
    }

    private static boolean matchesSource(
            PermissionAssignment assignment,
            Map<String, String> parameters,
            boolean includeInherited) {
        PermissionResource resource = assignment.getResource();
        return matches(parameters, "principal", assignment.getPrincipal().getId())
                && matches(parameters, "principalType", assignment.getPrincipal().getType().name())
                && matches(parameters, "access", assignment.getAccess())
                && (includeInherited
                        || (matches(parameters, "resourceType", resource.getType().name())
                                && matches(parameters, "database", resource.getDatabase())
                                && matches(parameters, "table", resource.getTable())
                                && matches(parameters, "function", resource.getFunction())
                                && matches(parameters, "view", resource.getView())));
    }

    private static boolean isEffectiveOn(
            PermissionAssignment assignment, PermissionResource target) {
        PermissionResource source = assignment.getResource();
        if (source.equals(target)) {
            return true;
        }
        if (!isAccessApplicableToTarget(assignment.getAccess(), target)) {
            return false;
        }
        if (assignment.getScope() == PermissionScope.SELF) {
            return false;
        }
        if (source.getType() == ResourceType.CATALOG) {
            return target.getType() != ResourceType.CATALOG;
        }
        return source.getType() == ResourceType.DATABASE
                && target.getType() != ResourceType.CATALOG
                && target.getType() != ResourceType.DATABASE
                && Objects.equals(source.getDatabase(), target.getDatabase());
    }

    private static boolean isAccessApplicableToTarget(String access, PermissionResource target) {
        try {
            PermissionAccess.canonicalize(target, PermissionScope.SELF, access);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static PermissionAssignment effectiveAssignment(
            PermissionAssignment assignment, PermissionResource target) {
        if (assignment.getResource().equals(target)) {
            return assignment;
        }
        return new PermissionAssignment(
                target,
                PermissionScope.SELF,
                assignment.getAccess(),
                assignment.getPrincipal(),
                assignment.getExpireTime(),
                assignment.getResource());
    }

    private static boolean matches(Map<String, String> parameters, String key, String value) {
        return !parameters.containsKey(key) || Objects.equals(parameters.get(key), value);
    }

    private static String sortKey(PermissionAssignment assignment) {
        PermissionResource source =
                assignment.getInheritedFrom() == null
                        ? assignment.getResource()
                        : assignment.getInheritedFrom();
        return source.getType().name()
                + '\0'
                + value(source.getDatabase())
                + '\0'
                + value(source.getTable())
                + '\0'
                + value(source.getFunction())
                + '\0'
                + value(source.getView())
                + '\0'
                + assignment.getScope().name()
                + '\0'
                + assignment.getAccess()
                + '\0'
                + assignment.getPrincipal().getType().name()
                + '\0'
                + assignment.getPrincipal().getId();
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }
}
