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

package org.apache.paimon.tests;

import org.junit.jupiter.api.BeforeEach;

import java.util.UUID;

/** Base e2e test class for flink action with kafka and without kafka. */
public abstract class FlinkActionsE2eTestBase extends E2eTestBase {

    public FlinkActionsE2eTestBase(boolean withKafka, boolean withHive) {
        super(withKafka, withHive);
    }

    protected String warehousePath;
    protected String catalogDdl;
    protected String useCatalogCmd;

    @BeforeEach
    public void setUp() {
        warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID() + ".store";
        catalogDdl =
                String.format(
                        "CREATE CATALOG ts_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'warehouse' = '%s'\n"
                                + ");",
                        warehousePath);

        useCatalogCmd = "USE CATALOG ts_catalog;";
    }

    protected void runBatchSql(String sql, String... ddls) throws Exception {
        runBatchSql(String.join("\n", ddls) + "\n" + sql);
    }

    protected void runStreamingSql(String sql, String... ddls) throws Exception {
        runStreamingSql(String.join("\n", ddls) + "\n" + sql);
    }
}
