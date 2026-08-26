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

import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionResource;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Atomic permission assignment store for the REST catalog test server. */
final class RESTPermissionStore {

    private final Map<PermissionKey, PermissionAssignment> assignments = new ConcurrentHashMap<>();

    void put(PermissionAssignment assignment) {
        assignments.put(PermissionKey.fromAssignment(assignment), assignment);
    }

    void remove(PermissionResource resource, String access, String principal) {
        assignments.remove(new PermissionKey(resource, access, principal));
    }

    List<PermissionAssignment> list(PermissionResource target, Map<String, String> parameters) {
        return assignments.values().stream()
                .filter(assignment -> assignment.getResource().equals(target))
                .filter(assignment -> matches(parameters, "principal", assignment.getPrincipal()))
                .filter(assignment -> matches(parameters, "access", assignment.getAccess()))
                .sorted(Comparator.comparing(RESTPermissionStore::sortKey))
                .collect(Collectors.toList());
    }

    private static boolean matches(Map<String, String> parameters, String key, String value) {
        return !parameters.containsKey(key) || parameters.get(key).equals(value);
    }

    private static String sortKey(PermissionAssignment assignment) {
        PermissionResource source = assignment.getResource();
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
                + assignment.getAccess()
                + '\0'
                + assignment.getPrincipal();
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }

    private static class PermissionKey {

        private final PermissionResource resource;
        private final String access;
        private final String principal;

        private PermissionKey(PermissionResource resource, String access, String principal) {
            this.resource = resource;
            this.access = access;
            this.principal = principal;
        }

        private static PermissionKey fromAssignment(PermissionAssignment assignment) {
            return new PermissionKey(
                    assignment.getResource(), assignment.getAccess(), assignment.getPrincipal());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PermissionKey)) {
                return false;
            }
            PermissionKey that = (PermissionKey) o;
            return resource.equals(that.resource)
                    && access.equals(that.access)
                    && principal.equals(that.principal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resource, access, principal);
        }
    }
}
