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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Atomic permission assignment store for the REST catalog test server. */
final class RESTPermissionStore {

    private final Map<PermissionKey, PermissionAssignment> assignments = new HashMap<>();

    synchronized <T> T executeAtomically(Supplier<T> operation) {
        return operation.get();
    }

    synchronized void put(PermissionAssignment assignment) {
        assignments.put(PermissionKey.fromAssignment(assignment), assignment);
    }

    synchronized void remove(PermissionResource resource, String access, String principal) {
        assignments.remove(new PermissionKey(resource, access, principal));
    }

    synchronized List<PermissionAssignment> list(
            PermissionResource target, Map<String, String> parameters) {
        return assignments.values().stream()
                .filter(assignment -> assignment.getResource().equals(target))
                .filter(assignment -> matches(parameters, "principal", assignment.getPrincipal()))
                .filter(assignment -> matches(parameters, "access", assignment.getAccess()))
                .sorted(Comparator.comparing(RESTPermissionStore::sortKey))
                .collect(Collectors.toList());
    }

    synchronized void renameTable(Identifier source, Identifier destination) {
        replaceResources(
                resource ->
                        isTableResource(resource, source, ResourceType.TABLE)
                                || isTableResource(resource, source, ResourceType.COLUMN),
                resource ->
                        new PermissionResource(
                                resource.getType(),
                                destination.getDatabaseName(),
                                destination.getTableName(),
                                null,
                                null));
    }

    synchronized void renameView(Identifier source, Identifier destination) {
        replaceResources(
                resource ->
                        resource.getType() == ResourceType.VIEW
                                && Objects.equals(source.getDatabaseName(), resource.getDatabase())
                                && Objects.equals(source.getObjectName(), resource.getView()),
                resource ->
                        new PermissionResource(
                                ResourceType.VIEW,
                                destination.getDatabaseName(),
                                null,
                                null,
                                destination.getObjectName()));
    }

    synchronized void removeTable(Identifier identifier) {
        removeResources(
                resource ->
                        isTableResource(resource, identifier, ResourceType.TABLE)
                                || isTableResource(resource, identifier, ResourceType.COLUMN));
    }

    synchronized void removeView(Identifier identifier) {
        removeResources(
                resource ->
                        resource.getType() == ResourceType.VIEW
                                && Objects.equals(
                                        identifier.getDatabaseName(), resource.getDatabase())
                                && Objects.equals(identifier.getObjectName(), resource.getView()));
    }

    synchronized void removeFunction(Identifier identifier) {
        removeResources(
                resource ->
                        resource.getType() == ResourceType.FUNCTION
                                && Objects.equals(
                                        identifier.getDatabaseName(), resource.getDatabase())
                                && Objects.equals(
                                        identifier.getObjectName(), resource.getFunction()));
    }

    synchronized void removeDatabase(String database) {
        removeResources(resource -> Objects.equals(database, resource.getDatabase()));
    }

    synchronized boolean hasColumnAssignments(Identifier identifier) {
        return assignments.values().stream()
                .map(PermissionAssignment::getResource)
                .anyMatch(resource -> isTableResource(resource, identifier, ResourceType.COLUMN));
    }

    synchronized boolean canEvolveTableColumns(
            Identifier identifier, TableSchema previous, TableSchema current) {
        Map<String, String> currentNamesByPreviousName =
                currentNamesByPreviousName(previous, current);
        return assignments.values().stream()
                .filter(
                        assignment ->
                                isTableResource(
                                        assignment.getResource(), identifier, ResourceType.COLUMN))
                .map(PermissionAssignment::getColumns)
                .allMatch(
                        columns ->
                                columns.getColumnNames() == null
                                        || columns.getColumnNames().stream()
                                                .anyMatch(
                                                        name ->
                                                                currentNamesByPreviousName.get(name)
                                                                        != null));
    }

    synchronized void evolveTableColumns(
            Identifier identifier, TableSchema previous, TableSchema current) {
        Map<String, String> currentNamesByPreviousName =
                currentNamesByPreviousName(previous, current);
        List<PermissionAssignment> columnAssignments =
                assignments.values().stream()
                        .filter(
                                assignment ->
                                        isTableResource(
                                                assignment.getResource(),
                                                identifier,
                                                ResourceType.COLUMN))
                        .collect(Collectors.toList());
        for (PermissionAssignment assignment : columnAssignments) {
            PermissionColumns columns = assignment.getColumns();
            List<String> source =
                    columns.getColumnNames() == null
                            ? columns.getExcludedColumnNames()
                            : columns.getColumnNames();
            List<String> evolved =
                    source.stream()
                            .map(currentNamesByPreviousName::get)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            assignments.remove(PermissionKey.fromAssignment(assignment));
            if (!evolved.isEmpty()) {
                PermissionColumns evolvedColumns =
                        columns.getColumnNames() == null
                                ? new PermissionColumns(null, evolved)
                                : new PermissionColumns(evolved, null);
                PermissionAssignment evolvedAssignment =
                        new PermissionAssignment(
                                assignment.getResource(),
                                assignment.getAccess(),
                                assignment.getPrincipal(),
                                evolvedColumns,
                                assignment.getExpireTime());
                assignments.put(PermissionKey.fromAssignment(evolvedAssignment), evolvedAssignment);
            }
        }
    }

    private static Map<String, String> currentNamesByPreviousName(
            TableSchema previous, TableSchema current) {
        Map<Integer, String> currentNamesById =
                current.fields().stream().collect(Collectors.toMap(DataField::id, DataField::name));
        Map<String, String> result = new HashMap<>();
        for (DataField field : previous.fields()) {
            result.put(field.name(), currentNamesById.get(field.id()));
        }
        return result;
    }

    private static boolean matches(Map<String, String> parameters, String key, String value) {
        return !parameters.containsKey(key) || parameters.get(key).equals(value);
    }

    static String sortKey(PermissionAssignment assignment) {
        PermissionResource source = assignment.getResource();
        return cursorPart(source.getType().name())
                + cursorPart(source.getDatabase())
                + cursorPart(source.getTable())
                + cursorPart(source.getFunction())
                + cursorPart(source.getView())
                + cursorPart(assignment.getAccess())
                + cursorPart(assignment.getPrincipal());
    }

    private static String cursorPart(String value) {
        return value == null ? "-1:" : value.length() + ":" + value;
    }

    private void replaceResources(
            Predicate<PermissionResource> matches,
            Function<PermissionResource, PermissionResource> replacement) {
        List<PermissionAssignment> replaced =
                assignments.values().stream()
                        .filter(assignment -> matches.test(assignment.getResource()))
                        .collect(Collectors.toList());
        for (PermissionAssignment assignment : replaced) {
            assignments.remove(PermissionKey.fromAssignment(assignment));
            PermissionAssignment newAssignment =
                    new PermissionAssignment(
                            replacement.apply(assignment.getResource()),
                            assignment.getAccess(),
                            assignment.getPrincipal(),
                            assignment.getColumns(),
                            assignment.getExpireTime());
            assignments.put(PermissionKey.fromAssignment(newAssignment), newAssignment);
        }
    }

    private void removeResources(Predicate<PermissionResource> matches) {
        assignments.entrySet().removeIf(entry -> matches.test(entry.getValue().getResource()));
    }

    private static boolean isTableResource(
            PermissionResource resource, Identifier identifier, ResourceType type) {
        return resource.getType() == type
                && Objects.equals(identifier.getDatabaseName(), resource.getDatabase())
                && Objects.equals(identifier.getTableName(), resource.getTable());
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
