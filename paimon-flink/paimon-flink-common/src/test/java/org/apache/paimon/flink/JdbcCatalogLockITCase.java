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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;

import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that SQL schema changes participate in the JDBC catalog's table locks. */
public class JdbcCatalogLockITCase extends AbstractTestBase {

    @Test
    void testSchemaChangesAcquireCatalogLock() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                        String.format(
                                "CREATE CATALOG jdbc WITH ("
                                        + "'type'='paimon', "
                                        + "'metastore'='jdbc', "
                                        + "'warehouse'='%s', "
                                        + "'uri'='jdbc:sqlite:file:%s?mode=memory&cache=shared', "
                                        + "'lock.enabled'='true', "
                                        + "'lock-acquire-timeout'='1 s', "
                                        + "'lock-check-max-sleep'='10 ms')",
                                getTempDirPath(), UUID.randomUUID()))
                .await();
        tEnv.useCatalog("jdbc");

        Identifier identifier = Identifier.create("default", "table1");
        String createTable = "CREATE TABLE table1 (a STRING, b STRING, c STRING)";
        String alterTable = "ALTER TABLE table1 ADD (d STRING)";
        try (Catalog catalog = ((FlinkCatalog) tEnv.getCatalog("jdbc").get()).catalog()) {
            assertSchemaChangeBlocked(tEnv, catalog, identifier, createTable);
            assertThat(catalog.listTables("default")).doesNotContain("table1");
            tEnv.executeSql(createTable).await();

            assertSchemaChangeBlocked(tEnv, catalog, identifier, alterTable);
            assertThat(catalog.getTable(identifier).rowType().getFieldNames())
                    .containsExactly("a", "b", "c");
            tEnv.executeSql(alterTable).await();
            assertThat(catalog.getTable(identifier).rowType().getFieldNames())
                    .containsExactly("a", "b", "c", "d");
        }
    }

    private void assertSchemaChangeBlocked(
            TableEnvironment tEnv, Catalog catalog, Identifier identifier, String statement)
            throws Exception {
        try (Lock lock = catalog.createLock(identifier, null, "other-writer", new Options())) {
            lock.runWithLock(
                    () -> {
                        assertThatThrownBy(() -> tEnv.executeSql(statement).await())
                                .rootCause()
                                .isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("Acquire lock failed");
                        return null;
                    });
        }
    }
}
