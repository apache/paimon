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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.commons.io.FileUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.CHANGE_TRACKING;
import static org.apache.flink.table.store.connector.TableStoreITCase.StatementType.CREATE_STATEMENT;
import static org.apache.flink.table.store.connector.TableStoreITCase.StatementType.DROP_STATEMENT;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.apache.flink.table.store.kafka.KafkaLogOptions.BOOTSTRAP_SERVERS;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end tests for table store. */
@RunWith(Parameterized.class)
public class TableStoreITCase extends KafkaTableTestBase {

    private static final String CURRENT_CATALOG = "catalog";
    private static final String CURRENT_DATABASE = "database";

    private final ObjectIdentifier tableIdentifier;
    private final StatementType statementType;
    private final boolean enableChangeTracking;
    private final boolean ignoreException;
    private final ExpectedResult expectedResult;

    private String rootPath;
    private ResolvedCatalogTable resolvedTable;
    @Rule public TestName name = new TestName();

    public TableStoreITCase(
            String tableName,
            StatementType statementType,
            boolean enableChangeTracking,
            boolean ignoreException,
            ExpectedResult expectedResult) {
        this.tableIdentifier = ObjectIdentifier.of(CURRENT_CATALOG, CURRENT_DATABASE, tableName);
        this.statementType = statementType;
        this.enableChangeTracking = enableChangeTracking;
        this.ignoreException = ignoreException;
        this.expectedResult = expectedResult;
    }

    @Parameterized.Parameters(
            name =
                    "tableName-{0}, statementType-{1}, enableChangeTracking-{2}, ignoreException-{3}, expectedResult-{4}")
    public static Collection<Object[]> data() {
        return Stream.concat(prepareCreateTableSpecs().stream(), prepareDropTableSpecs().stream())
                .collect(Collectors.toList());
    }

    @Before
    @Override
    public void setup() {
        super.setup();
        ((TableEnvironmentImpl) tEnv)
                .getCatalogManager()
                .registerCatalog(
                        CURRENT_CATALOG,
                        new GenericInMemoryCatalog(CURRENT_CATALOG, CURRENT_DATABASE));
        tEnv.useCatalog(CURRENT_CATALOG);
        resolvedTable =
                createResolvedTable(
                        Collections.emptyMap(),
                        RowType.of(new IntType(), new VarCharType()),
                        new int[0]);
        try {
            rootPath = TEMPORARY_FOLDER.newFolder().getPath();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        prepareSessionContext();
        // match parameter type with test name to conditionally skip before setup, because junit4
        // doesn't support multiple data providers for different methods
        if (name.getMethodName().startsWith("testCreateTable")
                && statementType == CREATE_STATEMENT) {
            prepareEnvForCreateTable();
        } else if (name.getMethodName().startsWith("testDropTable")
                && statementType == DROP_STATEMENT) {
            prepareEnvForDropTable();
        }
    }

    @Test
    public void testCreateTable() {
        Assume.assumeTrue(statementType == CREATE_STATEMENT);
        final String ddl =
                String.format(
                        "CREATE TABLE%s%s (f0 INT, f1 STRING)\n",
                        ignoreException ? " IF NOT EXISTS " : " ",
                        tableIdentifier.asSerializableString());
        if (expectedResult.success) {
            tEnv.executeSql(ddl);
            // check catalog
            assertThat(((TableEnvironmentImpl) tEnv).getCatalogManager().getTable(tableIdentifier))
                    .isPresent();
            // check table store
            assertThat(Paths.get(rootPath, getRelativeFileStoreTablePath(tableIdentifier)).toFile())
                    .exists();
            // check log store
            assertThat(topicExists(tableIdentifier.asSummaryString()))
                    .isEqualTo(enableChangeTracking);
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
                // throw exception when creating file path/topic, and catalog meta does not exist
                assertThat(
                                ((TableEnvironmentImpl) tEnv)
                                        .getCatalogManager()
                                        .getTable(tableIdentifier))
                        .isNotPresent();
            }
        }
    }

    @Test
    public void testDropTable() {
        Assume.assumeTrue(statementType == DROP_STATEMENT);
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
            // check table store
            assertThat(Paths.get(rootPath, getRelativeFileStoreTablePath(tableIdentifier)).toFile())
                    .doesNotExist();
            // check log store
            assertThat(topicExists(tableIdentifier.asSummaryString())).isFalse();
        } else {
            // check inconsistency between catalog/file store/log store
            assertThat(ignoreException).isFalse();
            if (ValidationException.class.isAssignableFrom(expectedResult.expectedType)) {
                // successfully delete path/topic, but schema doesn't exist in catalog
                assertThatThrownBy(() -> tEnv.executeSql(ddl))
                        .isInstanceOf(expectedResult.expectedType)
                        .hasMessageContaining(expectedResult.expectedMessage);
                assertThat(
                                ((TableEnvironmentImpl) tEnv)
                                        .getCatalogManager()
                                        .getTable(tableIdentifier))
                        .isNotPresent();
            } else {
                assertThatThrownBy(() -> tEnv.executeSql(ddl))
                        .getCause()
                        .isInstanceOf(expectedResult.expectedType)
                        .hasMessageContaining(expectedResult.expectedMessage);
                // throw exception when deleting file path/topic, so schema still exists in catalog
                assertThat(
                                ((TableEnvironmentImpl) tEnv)
                                        .getCatalogManager()
                                        .getTable(tableIdentifier))
                        .isPresent();
            }
        }
    }

    // ~ Tools ------------------------------------------------------------------

    private static List<Object[]> prepareCreateTableSpecs() {
        List<Object[]> specs = new ArrayList<>();
        // successful case specs
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
                    true,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
                    false,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
                    true,
                    false,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
                    false,
                    false,
                    new ExpectedResult().success(true)
                });

        // failed case specs
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
                    false,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableException.class)
                            .expectedMessage("Failed to create file store path.")
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    CREATE_STATEMENT,
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
                    tableName,
                    CREATE_STATEMENT,
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

    private static List<Object[]> prepareDropTableSpecs() {
        List<Object[]> specs = new ArrayList<>();
        // successful case specs
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    true,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    false,
                    true,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    true,
                    false,
                    new ExpectedResult().success(true)
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    false,
                    false,
                    new ExpectedResult().success(true)
                });

        // failed case specs
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    false,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableException.class)
                            .expectedMessage("Failed to delete file store path.")
                });
        specs.add(
                new Object[] {
                    "table_" + UUID.randomUUID(),
                    DROP_STATEMENT,
                    true,
                    false,
                    new ExpectedResult()
                            .success(false)
                            .expectedType(TableException.class)
                            .expectedMessage("Failed to delete kafka topic.")
                });
        final String tableName = "table_" + UUID.randomUUID();
        specs.add(
                new Object[] {
                    tableName,
                    DROP_STATEMENT,
                    true,
                    false,
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
                                                    .asSummaryString()))
                });
        return specs;
    }

    private void prepareSessionContext() {
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString(TABLE_STORE_PREFIX + FILE_PATH.key(), rootPath);
        configuration.setString(
                TABLE_STORE_PREFIX + LOG_PREFIX + BOOTSTRAP_SERVERS.key(), getBootstrapServers());
        configuration.setBoolean(TABLE_STORE_PREFIX + CHANGE_TRACKING.key(), enableChangeTracking);
    }

    private void prepareEnvForCreateTable() {
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
            if (enableChangeTracking && !ignoreException) {
                deleteTopicIfExists(tableIdentifier.asSummaryString());
            }
        } else if (expectedResult.expectedMessage.startsWith("Failed to create file store path.")) {
            // failed when creating file store
            Paths.get(rootPath, getRelativeFileStoreTablePath(tableIdentifier)).toFile().mkdirs();
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

    private void prepareEnvForDropTable() {
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
                // delete log store topic does not affect dropping the table
                if (enableChangeTracking) {
                    deleteTopicIfExists(tableIdentifier.asSummaryString());
                }
            }
        } else if (expectedResult.expectedMessage.startsWith("Failed to delete file store path.")) {
            // failed when deleting file path
            deleteTablePath();
        } else if (expectedResult.expectedMessage.startsWith("Failed to delete kafka topic.")) {
            // failed when deleting topic
            deleteTopicIfExists(tableIdentifier.asSummaryString());
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

    private void deleteTablePath() {
        try {
            FileUtils.deleteDirectory(
                    Paths.get(rootPath, getRelativeFileStoreTablePath(tableIdentifier)).toFile());
        } catch (IOException ignored) {
            // ignored
        }
    }

    static ResolvedCatalogTable createResolvedTable(
            Map<String, String> options, RowType rowType, int[] pk) {
        List<String> fieldNames = rowType.getFieldNames();
        List<DataType> fieldDataTypes =
                rowType.getChildren().stream()
                        .map(TypeConversions::fromLogicalToDataType)
                        .collect(Collectors.toList());
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromFields(fieldNames, fieldDataTypes).build(),
                        "a comment",
                        Collections.emptyList(),
                        options);
        List<Column> resolvedColumns =
                IntStream.range(0, fieldNames.size())
                        .mapToObj(i -> Column.physical(fieldNames.get(i), fieldDataTypes.get(i)))
                        .collect(Collectors.toList());
        UniqueConstraint constraint = null;
        if (pk.length > 0) {
            List<String> pkNames =
                    Arrays.stream(pk).mapToObj(fieldNames::get).collect(Collectors.toList());
            constraint = UniqueConstraint.primaryKey("pk", pkNames);
        }
        return new ResolvedCatalogTable(
                origin, new ResolvedSchema(resolvedColumns, Collections.emptyList(), constraint));
    }

    static String getRelativeFileStoreTablePath(ObjectIdentifier tableIdentifier) {
        return String.format(
                "root/%s.catalog/%s.db/%s",
                tableIdentifier.getCatalogName(),
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getObjectName());
    }

    enum StatementType {
        CREATE_STATEMENT,
        DROP_STATEMENT
    }

    private static class ExpectedResult {
        private boolean success;
        private Class<? extends Throwable> expectedType;
        private String expectedMessage;

        ExpectedResult success(boolean success) {
            this.success = success;
            return this;
        }

        ExpectedResult expectedType(Class<? extends Throwable> exceptionClazz) {
            this.expectedType = exceptionClazz;
            return this;
        }

        ExpectedResult expectedMessage(String exceptionMessage) {
            this.expectedMessage = exceptionMessage;
            return this;
        }

        @Override
        public String toString() {
            return "ExpectedResult{"
                    + "success="
                    + success
                    + ", expectedType="
                    + expectedType
                    + ", expectedMessage='"
                    + expectedMessage
                    + '\''
                    + '}';
        }
    }
}
