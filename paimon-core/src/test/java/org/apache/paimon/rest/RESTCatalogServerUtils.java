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
import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.rest.requests.DropPolicyRequest;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.rest.RESTApi.MAX_RESULTS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Stateless helpers used by {@link RESTCatalogServer}. */
final class RESTCatalogServerUtils {

    private RESTCatalogServerUtils() {}

    static <T> T parseRequest(String data, Class<T> requestClass) {
        try {
            return RESTApi.fromJson(data, requestClass);
        } catch (JsonProcessingException e) {
            Throwable invalidArgument = findCause(e, IllegalArgumentException.class);
            throw new InvalidRequestException(
                    errorMessage(invalidArgument == null ? e : invalidArgument), e);
        }
    }

    @Nullable
    static Throwable findCause(Throwable throwable, Class<? extends Throwable> causeType) {
        Throwable current = throwable;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return current;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return null;
    }

    static String errorMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (StringUtils.isNotEmpty(current.getMessage())) {
                return current.getMessage();
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return throwable.getClass().getSimpleName();
    }

    static <T> PagedList<T> buildManagementPage(
            List<T> elements,
            int maxResults,
            @Nullable String pageToken,
            Function<T, String> sortKey) {
        String after = decodeManagementPageToken(pageToken);
        List<T> remaining =
                elements.stream()
                        .sorted(Comparator.comparing(sortKey))
                        .filter(
                                element ->
                                        after == null
                                                || sortKey.apply(element).compareTo(after) > 0)
                        .collect(Collectors.toList());
        int end = Math.min(maxResults, remaining.size());
        List<T> page = new ArrayList<>(remaining.subList(0, end));
        String nextPageToken =
                end < remaining.size()
                        ? encodeManagementPageToken(sortKey.apply(page.get(page.size() - 1)))
                        : null;
        return new PagedList<>(page, nextPageToken);
    }

    static PermissionResource permissionResource(Map<String, String> parameters) {
        return new PermissionResource(
                ResourceType.fromString(parameters.get("resourceType")),
                parameters.get("database"),
                parameters.get("table"),
                parameters.get("function"),
                parameters.get("view"));
    }

    static String resourceName(PermissionResource resource) {
        switch (resource.getType()) {
            case CATALOG:
            case CATALOG_ALL:
                return "catalog";
            case DATABASE:
            case DATABASE_ALL:
                return resource.getDatabase();
            case TABLE:
            case COLUMN:
                return resource.getDatabase() + "." + resource.getTable();
            case FUNCTION:
                return resource.getDatabase() + "." + resource.getFunction();
            case VIEW:
                return resource.getDatabase() + "." + resource.getView();
            default:
                return resource.getType().name();
        }
    }

    static int getPermissionMaxResults(Map<String, String> parameters) {
        String strMaxResults = parameters.get(MAX_RESULTS);
        if (strMaxResults == null) {
            return RESTCatalogServer.DEFAULT_MAX_RESULTS;
        }
        int maxResults = Integer.parseInt(strMaxResults);
        return Math.max(1, Math.min(maxResults, ListPermissionsRequest.MAX_PAGE_SIZE));
    }

    static String policyResourceName(DataPolicy policy) {
        ColumnMask columnMask = policy.getColumnMask();
        return policy.type().name()
                + ":"
                + policy.getPrincipal()
                + (columnMask == null ? "" : ":" + columnMask.getOnColumn());
    }

    static String policyResourceName(DropPolicyRequest request) {
        return request.getType().name()
                + ":"
                + request.getPrincipal()
                + (request.getColumn() == null ? "" : ":" + request.getColumn());
    }

    static DataPolicy withResource(DataPolicy policy, PermissionResource resource) {
        return policy.getRowFilter() == null
                ? DataPolicy.columnMask(resource, policy.getColumnMask(), policy.getPrincipal())
                : DataPolicy.rowFilter(resource, policy.getRowFilter(), policy.getPrincipal());
    }

    static DataPolicy canonicalizePolicy(DataPolicy policy, TableSchema schema) {
        if (policy.getRowFilter() != null) {
            String predicate =
                    JsonSerdeUtil.toFlatJson(parseRowFilter(schema, policy.getRowFilter()));
            return DataPolicy.rowFilter(
                    policy.getResource(), new RowFilter(predicate), policy.getPrincipal());
        }
        ColumnMask columnMask = policy.getColumnMask();
        String transform = JsonSerdeUtil.toFlatJson(parseColumnMask(schema, columnMask));
        return DataPolicy.columnMask(
                policy.getResource(),
                new ColumnMask(columnMask.getOnColumn(), transform),
                policy.getPrincipal());
    }

    static Predicate parseRowFilter(TableSchema schema, RowFilter rowFilter) {
        Predicate predicate = JsonSerdeUtil.fromJson(rowFilter.getPredicate(), Predicate.class);
        checkArgument(predicate != null, "Row filter predicate cannot be JSON null.");
        Predicate remapped =
                TableQueryAuthResult.remapPredicate(predicate, schema.logicalRowType());
        checkArgument(remapped != null, "Row filter predicate cannot be empty.");
        return remapped;
    }

    static Transform parseColumnMask(TableSchema schema, ColumnMask columnMask) {
        Transform transform = JsonSerdeUtil.fromJson(columnMask.getTransform(), Transform.class);
        checkArgument(transform != null, "Column mask transform cannot be JSON null.");
        RowType rowType = schema.logicalRowType();
        List<Object> remappedInputs = new ArrayList<>();
        for (Object input : transform.inputs()) {
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                int index = rowType.getFieldIndex(ref.name());
                checkArgument(
                        index >= 0,
                        "Column masking refers to field '%s' which is not present in table schema.",
                        ref.name());
                remappedInputs.add(new FieldRef(index, ref.name(), rowType.getTypeAt(index)));
            } else {
                remappedInputs.add(input);
            }
        }
        Transform remapped = transform.copyWithNewInputs(remappedInputs);
        int targetIndex = rowType.getFieldIndex(columnMask.getOnColumn());
        checkArgument(
                targetIndex >= 0,
                "Policy column %s does not exist in table schema.",
                columnMask.getOnColumn());
        checkArgument(
                rowType.getTypeAt(targetIndex).equals(remapped.outputType()),
                "Column mask output type %s does not match target column %s type %s.",
                remapped.outputType(),
                columnMask.getOnColumn(),
                rowType.getTypeAt(targetIndex));
        return remapped;
    }

    static void validatePoliciesForSchema(
            Identifier identifier,
            @Nullable String tableUuid,
            TableSchema schema,
            Map<PolicyKey, DataPolicy> policyStore) {
        if (tableUuid == null) {
            return;
        }
        List<DataPolicy> policies =
                policyStore.entrySet().stream()
                        .filter(entry -> entry.getKey().tableUuid.equals(tableUuid))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
        if (policies.isEmpty()) {
            return;
        }
        checkArgument(
                CoreOptions.fromMap(schema.options()).queryAuthEnabled(),
                "Cannot disable query-auth.enabled while table %s has data policies.",
                identifier.getFullName());

        Set<String> columns = new HashSet<>(schema.fieldNames());
        for (DataPolicy policy : policies) {
            ColumnMask columnMask = policy.getColumnMask();
            if (columnMask != null) {
                checkArgument(
                        columns.contains(columnMask.getOnColumn()),
                        "Cannot remove or rename policy column %s from table %s.",
                        columnMask.getOnColumn(),
                        identifier.getFullName());
            }
            if (policy.getRowFilter() == null) {
                parseColumnMask(schema, columnMask);
            } else {
                parseRowFilter(schema, policy.getRowFilter());
            }
        }
    }

    static void validatePermissionsForSchema(
            Identifier identifier,
            TableSchema previous,
            TableSchema current,
            RESTPermissionStore permissionStore) {
        if (permissionStore.hasColumnAssignments(identifier)) {
            checkArgument(
                    CoreOptions.fromMap(current.options()).queryAuthEnabled(),
                    "Cannot disable query-auth.enabled while table %s has column permissions.",
                    identifier.getFullName());
            checkArgument(
                    permissionStore.canEvolveTableColumns(identifier, previous, current),
                    "Cannot drop every allowed column while table %s has column permissions.",
                    identifier.getFullName());
        }
    }

    static boolean matchesPolicy(DataPolicy policy, Map<String, String> parameters) {
        if (!matches(parameters, "type", policy.type().name())) {
            return false;
        }
        if (parameters.containsKey("column")) {
            ColumnMask columnMask = policy.getColumnMask();
            if (columnMask == null
                    || !Objects.equals(parameters.get("column"), columnMask.getOnColumn())) {
                return false;
            }
        }
        return !parameters.containsKey("principal")
                || policy.getPrincipal().equals(parameters.get("principal"));
    }

    static PolicyPath policyPath(String resourcePath, String permissionUri) {
        String catalogBase = StringUtils.substringBeforeLast(permissionUri, "/");
        checkArgument(resourcePath.startsWith(catalogBase + "/"), "Not a catalog policy path.");
        String[] parts = resourcePath.substring(catalogBase.length() + 1).split("/");
        if ((parts.length == 5 || (parts.length == 6 && "drop".equals(parts[5])))
                && "databases".equals(parts[0])
                && "tables".equals(parts[2])
                && "policies".equals(parts[4])) {
            return new PolicyPath(
                    new PermissionResource(
                            ResourceType.TABLE,
                            RESTUtil.decodeString(parts[1]),
                            RESTUtil.decodeString(parts[3]),
                            null,
                            null),
                    parts.length == 6);
        }
        throw new IllegalArgumentException("Not a policy path.");
    }

    @Nullable
    private static String decodeManagementPageToken(@Nullable String pageToken) {
        if (pageToken == null) {
            return null;
        }
        String decoded;
        try {
            decoded = new String(Base64.getUrlDecoder().decode(pageToken), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid management page token.", e);
        }
        checkArgument(decoded.startsWith("v1\0"), "Invalid management page token version.");
        return decoded.substring(3);
    }

    private static String encodeManagementPageToken(String sortKey) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(("v1\0" + sortKey).getBytes(StandardCharsets.UTF_8));
    }

    private static boolean matches(
            Map<String, String> parameters, String key, @Nullable String value) {
        return !parameters.containsKey(key) || Objects.equals(parameters.get(key), value);
    }

    static final class InvalidRequestException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private InvalidRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static final class PolicyPath {

        final PermissionResource resource;
        final boolean drop;

        private PolicyPath(PermissionResource resource, boolean drop) {
            this.resource = resource;
            this.drop = drop;
        }
    }
}
