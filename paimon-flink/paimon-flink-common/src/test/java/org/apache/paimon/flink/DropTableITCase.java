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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** IT cases for testing drop managed table ddl. */
public class DropTableITCase extends FlinkTestBase {

    @Override
    public void prepareEnv(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean ignoreException,
            String manualCause,
            ExpectedResult expectedResult) {
        super.prepareEnv(executionMode, tableName, ignoreException, manualCause, expectedResult);

        ((TableEnvironmentImpl) tEnv)
                .getCatalogManager()
                .createTable(resolvedTable, tableIdentifier, false);
        if (expectedResult.success) {
            if (ignoreException) {
                // delete catalog schema does not affect dropping the table
                tEnv.getCatalog(tEnv.getCurrentCatalog())
                        .ifPresent(
                                (catalog) -> {
                                    try {
                                        catalog.dropTable(tableIdentifier.toObjectPath(), false);
                                    } catch (TableNotExistException ignored) {
                                        // ignored
                                    }
                                });
                // delete file store path does not affect dropping the table
                deleteTablePath();
            }
        } else if (manualCause.contains("delete file store path")) {
            // failed when deleting file path
            deleteTablePath();
        } else {
            // failed when dropping catalog schema
            tEnv.getCatalog(tEnv.getCurrentCatalog())
                    .ifPresent(
                            (catalog) -> {
                                try {
                                    catalog.dropTable(tableIdentifier.toObjectPath(), false);
                                } catch (TableNotExistException ignored) {
                                    // ignored
                                }
                            });
        }
    }

    @Override
    protected void testCore() {
        String ddl =
                String.format(
                        "DROP TABLE%s%s\n",
                        ignoreException ? " IF EXISTS " : " ",
                        tableIdentifier.asSerializableString());

        if (expectedResult.success) {
            tEnv.executeSql(ddl);
            // check catalog
            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isNotPresent();
            // check paimon
            assertThat(Paths.get(rootPath, relativeTablePath(tableIdentifier)).toFile())
                    .doesNotExist();
        } else {
            // check inconsistency
            assertThat(ignoreException).isFalse();

            assertThatThrownBy(() -> tEnv.executeSql(ddl))
                    .isInstanceOf(expectedResult.expectedType)
                    .hasMessageContaining(expectedResult.expectedMessage);

            // check that table is not exist after deleting table path or dropping table
            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isNotPresent();
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

        // failed case specs
        String tableName = "table_" + UUID.randomUUID();
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        tableName,
                        false,
                        "delete file store path",
                        new ExpectedResult()
                                .success(false)
                                .expectedType(ValidationException.class)
                                .expectedMessage(
                                        String.format(
                                                "Table with identifier '%s' does not exist.",
                                                ObjectIdentifier.of(
                                                                CURRENT_CATALOG,
                                                                CURRENT_DATABASE,
                                                                tableName)
                                                        .asSummaryString()))));
        tableName = "table_" + UUID.randomUUID();
        args.add(
                arguments(
                        RuntimeExecutionMode.STREAMING,
                        tableName,
                        false,
                        "drop table",
                        new ExpectedResult()
                                .success(false)
                                .expectedType(ValidationException.class)
                                .expectedMessage(
                                        String.format(
                                                "Table with identifier '%s' does not exist.",
                                                ObjectIdentifier.of(
                                                                CURRENT_CATALOG,
                                                                CURRENT_DATABASE,
                                                                tableName)
                                                        .asSummaryString()))));

        return args;
    }
}
