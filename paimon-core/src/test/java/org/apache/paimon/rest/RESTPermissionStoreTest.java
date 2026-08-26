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
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
}
