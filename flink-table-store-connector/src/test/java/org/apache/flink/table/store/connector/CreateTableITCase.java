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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.table.store.CoreOptions.BUCKET;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.relativeTablePath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for testing create managed table ddl. */
@RunWith(Parameterized.class)
public class CreateTableITCase extends TableStoreTestBase {

    protected final boolean ignoreException;

    protected ResolvedCatalogTable resolvedTable =
            createResolvedTable(
                    Collections.emptyMap(),
                    RowType.of(new IntType(), new VarCharType()),
                    Collections.emptyList(),
                    Collections.emptyList());

    public CreateTableITCase(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean enableLogStore,
            boolean ignoreException,
            ExpectedResult expectedResult) {
        super(executionMode, tableName, enableLogStore, expectedResult);
        this.ignoreException = ignoreException;
    }

    @Test
    public void testCreateTable() {
        final String ddl =
                ShowCreateUtil.buildShowCreateTable(
                        resolvedTable, tableIdentifier, ignoreException);
        if (expectedResult.success) {
            tEnv.executeSql(ddl);
            // check catalog
            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isPresent();
            // check table store
            assertThat(Paths.get(rootPath, relativeTablePath(tableIdentifier)).toFile()).exists();
            // check log store
            assertThat(topicExists(tableIdentifier.asSummaryString())).isEqualTo(enableLogStore);
        } else {
            // check inconsistency between catalog/file store/log store
            assertThat(ignoreException).isFalse();
            assertThatThrownBy(() -> tEnv.executeSql(ddl))
                    .getCause()
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);

            if (expectedResult.expectedMessage.contains(
                    String.format("already exists in Catalog %s", CURRENT_CATALOG))) {
                assertThat(
                                ((TableEnvironmentImpl) tEnv)
                                        .getCatalogManager()
                                        .getTable(tableIdentifier))
                        .isPresent();
            } else {
                // throw exception when creating file path/topic, and catalog meta does not
                // exist
                assertThat(
                                ((TableEnvironmentImpl) tEnv)
                                        .getCatalogManager()
                                        .getTable(tableIdentifier))
                        .isNotPresent();
            }
        }
    }

    @Override
    public void prepareEnv() {
        if (expectedResult.success) {
            // ensure catalog doesn't contain the table meta
            tEnv.getCatalog(tEnv.getCurrentCatalog())
                    .ifPresent(
                            (catalog) -> {
                                try {
                                    catalog.dropTable(tableIdentifier.toObjectPath(), false);
                                } catch (TableNotExistException ignored) {
                                    // ignored
                                }
                            });
            // ensure log store doesn't exist the topic
            if (enableLogStore && !ignoreException) {
                deleteTopicIfExists(tableIdentifier.asSummaryString());
            }
        } else if (expectedResult.expectedMessage.startsWith("Failed to create file store path.")) {
            // failed when creating file store
            Paths.get(rootPath, relativeTablePath(tableIdentifier)).toFile().mkdirs();
        } else if (expectedResult.expectedMessage.startsWith("Failed to create kafka topic.")) {
            // failed when creating log store
            createTopicIfNotExists(tableIdentifier.asSummaryString(), BUCKET.defaultValue());
        } else {
            // failed when registering schema to catalog
            tEnv.getCatalog(tEnv.getCurrentCatalog())
                    .ifPresent(
                            (catalog) -> {
                                try {
                                    catalog.createTable(
                                            tableIdentifier.toObjectPath(), resolvedTable, false);
                                } catch (TableAlreadyExistException
                                        | DatabaseNotExistException ignored) {
                                    // ignored
                                }
                            });
        }
    }

    @Parameterized.Parameters(
            name =
                    "executionMode-{0}, tableName-{1}, enableLogStore-{2}, ignoreException-{3}, expectedResult-{4}")
    public static List<Object[]> data() {
        List<Object[]> specs = new ArrayList<>();
        // successful case specs
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    true,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    false,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    true,
                    false,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    false,
                    false,
                    new ExpectedResult().success(true)
                });

        // failed case specs
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    false,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableException.class)
                            .expectedMessage("Failed to create file store path.")
                });
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    "table_" + UUID.randomUUID(),
                    true,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableException.class)
                            .expectedMessage("Failed to create kafka topic.")
                });

        final String tableName = "table_" + UUID.randomUUID();
        specs.add(
                new Object[] {
                    RuntimeExecutionMode.STREAMING,
                    tableName,
                    true,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableAlreadyExistException.class)
                            .expectedMessage(
                                    String.format(
                                            "Table (or view) %s already exists in Catalog %s.",
                                            ObjectIdentifier.of(
                                                            CURRENT_CATALOG,
                                                            CURRENT_DATABASE,
                                                            tableName)
                                                    .toObjectPath()
                                                    .getFullName(),
                                            CURRENT_CATALOG))
                });
        return specs;
    }
}
