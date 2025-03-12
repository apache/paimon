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

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link ResourcePaths}. */
public class ResourcePathsTest {

    @Test
    public void testUrlEncode() {
        String database = "test_db";
        String objectName = "test_table$snapshot";
        ResourcePaths resourcePaths = new ResourcePaths("paimon");
        assertEquals(
                "/v1/paimon/databases/test_db/tables/test_table%24snapshot",
                resourcePaths.table(database, objectName));

        resourcePaths = new ResourcePaths("paimon/aaaa");
        assertEquals(
                "/v1/paimon%2Faaaa/databases/test_db/tables/test_table%24snapshot",
                resourcePaths.table(database, objectName));
    }
}
