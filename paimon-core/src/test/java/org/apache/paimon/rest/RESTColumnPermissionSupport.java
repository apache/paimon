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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.rest.responses.ErrorResponse;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Column permission validation and composition used by the REST catalog test server. */
final class RESTColumnPermissionSupport {

    private RESTColumnPermissionSupport() {}

    static boolean canSelect(
            RESTPermissionStore store,
            Set<String> principals,
            Identifier identifier,
            TableMetadata metadata,
            @Nullable List<String> selectedColumns) {
        PermissionResource resource =
                new PermissionResource(
                        ResourceType.COLUMN,
                        identifier.getDatabaseName(),
                        identifier.getTableName(),
                        null,
                        null);
        List<PermissionAssignment> assignments =
                store.list(resource, Collections.emptyMap()).stream()
                        .filter(assignment -> principals.contains(assignment.getPrincipal()))
                        .filter(RESTColumnPermissionSupport::notExpired)
                        .collect(Collectors.toList());
        if (assignments.isEmpty()) {
            return true;
        }

        Set<String> included = new HashSet<>(metadata.schema().fieldNames());
        for (PermissionAssignment assignment : assignments) {
            PermissionColumns columns = assignment.getColumns();
            if (columns.getColumnNames() != null) {
                included.retainAll(columns.getColumnNames());
            } else {
                included.removeAll(columns.getExcludedColumnNames());
            }
        }
        List<String> selected =
                selectedColumns == null ? metadata.schema().fieldNames() : selectedColumns;
        for (String column : selected) {
            if (!included.contains(column)) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    static ValidationError validate(PermissionAssignment assignment, TableMetadata metadata) {
        if (!CoreOptions.fromMap(metadata.schema().options()).queryAuthEnabled()) {
            return new ValidationError(
                    ErrorResponse.RESOURCE_TYPE_TABLE,
                    assignment.getResource().getDatabase()
                            + "."
                            + assignment.getResource().getTable(),
                    "Column permissions require query-auth.enabled=true.",
                    409);
        }
        Set<String> tableColumns = new HashSet<>(metadata.schema().fieldNames());
        PermissionColumns columns = assignment.getColumns();
        List<String> referenced =
                columns.getColumnNames() == null
                        ? columns.getExcludedColumnNames()
                        : columns.getColumnNames();
        for (String column : referenced) {
            if (!tableColumns.contains(column)) {
                return new ValidationError(
                        ErrorResponse.RESOURCE_TYPE_COLUMN,
                        column,
                        "Permission column does not exist.",
                        404);
            }
        }
        return null;
    }

    private static boolean notExpired(PermissionAssignment assignment) {
        return assignment.getExpireTime() == null
                || java.time.Instant.now()
                        .isBefore(java.time.Instant.parse(assignment.getExpireTime()));
    }

    static final class ValidationError {
        final String resourceType;
        final String resourceName;
        final String message;
        final int code;

        private ValidationError(
                String resourceType, String resourceName, String message, int code) {
            this.resourceType = resourceType;
            this.resourceName = resourceName;
            this.message = message;
            this.code = code;
        }
    }
}
