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
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests atomic replacement and exact-resource filtering in {@link RESTPermissionStore}. */
class RESTPermissionStoreTest {

    private static final String ANALYST = "analyst";

    @Test
    void testConcurrentGrantReplacesTheSameIdentity() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource table = tableResource();

        IntStream.range(0, 1000)
                .parallel()
                .forEach(
                        i ->
                                store.put(
                                        new PermissionAssignment(
                                                table,
                                                "SELECT",
                                                ANALYST,
                                                Instant.ofEpochSecond(i).toString())));

        assertThat(store.list(table, tableParameters())).hasSize(1);
    }

    @Test
    void testAtomicMutationCannotRaceLifecycleCleanup() throws Exception {
        RESTPermissionStore store = new RESTPermissionStore();
        Identifier identifier = Identifier.create("sales", "orders");
        PermissionAssignment assignment =
                new PermissionAssignment(tableResource(), "SELECT", ANALYST, null);
        CountDownLatch mutationStarted = new CountDownLatch(1);
        CountDownLatch releaseMutation = new CountDownLatch(1);
        CountDownLatch cleanupStarted = new CountDownLatch(1);

        CompletableFuture<Void> mutation =
                CompletableFuture.runAsync(
                        () ->
                                store.executeAtomically(
                                        () -> {
                                            mutationStarted.countDown();
                                            await(releaseMutation);
                                            store.put(assignment);
                                            return null;
                                        }));
        assertThat(mutationStarted.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> cleanup =
                CompletableFuture.runAsync(
                        () -> {
                            cleanupStarted.countDown();
                            store.removeTable(identifier);
                        });
        assertThat(cleanupStarted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(cleanup).isNotDone();

        releaseMutation.countDown();
        CompletableFuture.allOf(mutation, cleanup).get(10, TimeUnit.SECONDS);

        assertThat(store.list(tableResource(), Collections.emptyMap())).isEmpty();
    }

    @Test
    void testListReturnsOnlyTheExactTarget() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, null, null, null, null);
        store.put(new PermissionAssignment(catalog, "CREATEDATABASE", ANALYST, null));
        store.put(new PermissionAssignment(tableResource(), "SELECT", ANALYST, null));

        assertThat(store.list(tableResource(), tableParameters()))
                .singleElement()
                .extracting(PermissionAssignment::getAccess)
                .isEqualTo("SELECT");
    }

    @Test
    void testCursorKeyDoesNotFlattenOpaqueResourceParts() {
        PermissionAssignment first =
                new PermissionAssignment(
                        new PermissionResource(ResourceType.TABLE, "a", "b\0c", null, null),
                        "SELECT",
                        ANALYST,
                        null);
        PermissionAssignment second =
                new PermissionAssignment(
                        new PermissionResource(ResourceType.TABLE, "a\0b", "c", null, null),
                        "SELECT",
                        ANALYST,
                        null);

        assertThat(RESTPermissionStore.sortKey(first))
                .isNotEqualTo(RESTPermissionStore.sortKey(second));
    }

    @Test
    void testColumnGrantReplacesTheWholeColumnRangeForTheSameIdentity() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource column = columnResource();
        store.put(
                new PermissionAssignment(
                        column,
                        "SELECT",
                        ANALYST,
                        new PermissionColumns(Arrays.asList("id", "region"), null),
                        null));
        store.put(
                new PermissionAssignment(
                        column,
                        "SELECT",
                        ANALYST,
                        new PermissionColumns(null, Arrays.asList("email")),
                        null));

        assertThat(store.list(column, columnParameters()))
                .singleElement()
                .extracting(PermissionAssignment::getColumns)
                .extracting(PermissionColumns::getExcludedColumnNames)
                .isEqualTo(Arrays.asList("email"));
    }

    @Test
    void testTableAndColumnAssignmentsFollowResourceAndSchemaLifecycle() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource table = tableResource();
        PermissionResource column = columnResource();
        store.put(new PermissionAssignment(table, "SELECT", ANALYST, null));
        store.put(
                new PermissionAssignment(
                        column,
                        "SELECT",
                        ANALYST,
                        new PermissionColumns(null, Collections.singletonList("email")),
                        null));

        Identifier source = Identifier.create("sales", "orders");
        Identifier destination = Identifier.create("sales", "renamed_orders");
        store.renameTable(source, destination);

        PermissionResource renamedTable =
                new PermissionResource(ResourceType.TABLE, "sales", "renamed_orders", null, null);
        PermissionResource renamedColumn =
                new PermissionResource(ResourceType.COLUMN, "sales", "renamed_orders", null, null);
        assertThat(store.list(table, Collections.emptyMap())).isEmpty();
        assertThat(store.list(renamedTable, Collections.emptyMap()))
                .singleElement()
                .extracting(PermissionAssignment::getResource)
                .isEqualTo(renamedTable);

        TableSchema original = tableSchema(new DataField(1, "email", DataTypes.STRING()));
        TableSchema renamed = tableSchema(new DataField(1, "contact", DataTypes.STRING()));
        store.evolveTableColumns(destination, original, renamed);
        assertThat(store.list(renamedColumn, Collections.emptyMap()))
                .singleElement()
                .extracting(PermissionAssignment::getColumns)
                .extracting(PermissionColumns::getExcludedColumnNames)
                .isEqualTo(Collections.singletonList("contact"));

        store.put(
                new PermissionAssignment(
                        renamedColumn,
                        "SELECT",
                        ANALYST,
                        new PermissionColumns(Collections.singletonList("contact"), null),
                        null));
        assertThat(store.canEvolveTableColumns(destination, renamed, tableSchema())).isFalse();
        store.put(
                new PermissionAssignment(
                        renamedColumn,
                        "SELECT",
                        ANALYST,
                        new PermissionColumns(null, Collections.singletonList("contact")),
                        null));
        store.evolveTableColumns(destination, renamed, tableSchema());
        assertThat(store.list(renamedColumn, Collections.emptyMap())).isEmpty();

        store.removeTable(destination);
        assertThat(store.list(renamedTable, Collections.emptyMap())).isEmpty();
    }

    @Test
    void testDroppingDatabaseRemovesDirectAndChildAssignments() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, null, null, null, null);
        PermissionResource database =
                new PermissionResource(ResourceType.DATABASE, "sales", null, null, null);
        PermissionResource databaseAll =
                new PermissionResource(ResourceType.DATABASE_ALL, "sales", null, null, null);
        PermissionResource view =
                new PermissionResource(ResourceType.VIEW, "sales", null, null, "orders_view");
        PermissionResource function =
                new PermissionResource(ResourceType.FUNCTION, "sales", null, "orders_fn", null);
        for (PermissionResource resource :
                Arrays.asList(catalog, database, databaseAll, tableResource(), view, function)) {
            store.put(new PermissionAssignment(resource, "ALL", ANALYST, null));
        }

        store.removeDatabase("sales");

        for (PermissionResource resource :
                Arrays.asList(database, databaseAll, tableResource(), view, function)) {
            assertThat(store.list(resource, Collections.emptyMap())).isEmpty();
        }
        assertThat(store.list(catalog, Collections.emptyMap())).hasSize(1);
    }

    @Test
    void testViewAssignmentFollowsRenameAndDrop() {
        RESTPermissionStore store = new RESTPermissionStore();
        Identifier source = Identifier.create("sales", "orders_view");
        Identifier destination = Identifier.create("sales", "renamed_view");
        PermissionResource sourceResource =
                new PermissionResource(ResourceType.VIEW, "sales", null, null, "orders_view");
        PermissionResource destinationResource =
                new PermissionResource(ResourceType.VIEW, "sales", null, null, "renamed_view");
        store.put(new PermissionAssignment(sourceResource, "SELECT", ANALYST, null));

        store.renameView(source, destination);

        assertThat(store.list(sourceResource, Collections.emptyMap())).isEmpty();
        assertThat(store.list(destinationResource, Collections.emptyMap())).hasSize(1);

        store.removeView(destination);
        assertThat(store.list(destinationResource, Collections.emptyMap())).isEmpty();
    }

    private static PermissionResource tableResource() {
        return new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null);
    }

    private static PermissionResource columnResource() {
        return new PermissionResource(ResourceType.COLUMN, "sales", "orders", null, null);
    }

    private static Map<String, String> tableParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("resourceType", "TABLE");
        parameters.put("database", "sales");
        parameters.put("table", "orders");
        return parameters;
    }

    private static Map<String, String> columnParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("resourceType", "COLUMN");
        parameters.put("database", "sales");
        parameters.put("table", "orders");
        return parameters;
    }

    private static TableSchema tableSchema(DataField... fields) {
        int highestFieldId = Arrays.stream(fields).mapToInt(DataField::id).max().orElse(0);
        return new TableSchema(
                1,
                Arrays.asList(fields),
                highestFieldId,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
