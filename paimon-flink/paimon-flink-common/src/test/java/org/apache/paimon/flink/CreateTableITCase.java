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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** IT cases for testing create managed table ddl. */
public class CreateTableITCase extends FlinkTestBase {

    @Override
    public void prepareEnv(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean ignoreException,
            String manualCause,
            ExpectedResult expectedResult) {
        super.prepareEnv(executionMode, tableName, ignoreException, manualCause, expectedResult);

        if (expectedResult.success) {
            // ensure that table can be created successfully when table path exists
            if (manualCause.contains("create file store path")) {
                assertThat(
                                Paths.get(rootPath, relativeTablePath(tableIdentifier))
                                        .toFile()
                                        .mkdirs())
                        .isTrue();
            }
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

    @Override
    protected void testCore() {
        final String ddl =
                ShowCreateUtil.buildShowCreateTable(
                        resolvedTable, tableIdentifier, ignoreException);
        if (expectedResult.success) {
            tEnv.executeSql(ddl);
            // check catalog
            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isPresent();
            // check paimon
            assertThat(Paths.get(rootPath, relativeTablePath(tableIdentifier)).toFile()).exists();
        } else {
            // check inconsistency
            assertThat(ignoreException).isFalse();
            assertThatThrownBy(() -> tEnv.executeSql(ddl))
                    .getCause()
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);

            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isPresent();
        }
    }

    private static List<Arguments> data() {
        List<Arguments> args = new ArrayList<>();
        // successful case specs
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        "table_" + UUID.randomUUID(),
                        true,
                        "",
                        new ExpectedResult().success(true)));
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        "table_" + UUID.randomUUID(),
                        false,
                        "",
                        new ExpectedResult().success(true)));
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        "table_" + UUID.randomUUID(),
                        false,
                        "create file store path",
                        new ExpectedResult().success(true)));

        // failed case specs
        final String tableName = "table_" + UUID.randomUUID();
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        tableName,
                        false,
                        "",
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
                                                CURRENT_CATALOG))));

        return args;
    }
}
