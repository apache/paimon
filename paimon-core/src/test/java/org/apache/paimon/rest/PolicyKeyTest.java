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

import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;

import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for policy identities used by the REST catalog test server. */
public class PolicyKeyTest {

    @Test
    void testOrderingDoesNotFlattenOpaquePrincipalAndColumn() {
        PermissionResource resource =
                new PermissionResource(ResourceType.TABLE, "database", "table", null, null);
        PolicyKey first =
                new PolicyKey(
                        "table-id",
                        DataPolicy.columnMask(resource, new ColumnMask("mask", "c", null), "a:b"));
        PolicyKey second =
                new PolicyKey(
                        "table-id",
                        DataPolicy.columnMask(resource, new ColumnMask("mask", "b:c", null), "a"));

        TreeSet<PolicyKey> sorted = new TreeSet<>();
        sorted.add(first);
        sorted.add(second);

        assertThat(sorted).containsExactly(second, first);
    }
}
