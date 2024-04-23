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

import org.apache.paimon.flink.util.AbstractTestBase;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** End-to-end test base for paimon. */
public abstract class FlinkTestBase extends AbstractTestBase {

    public static final String CURRENT_CATALOG = "catalog";
    public static final String CURRENT_DATABASE = "default";

    protected ObjectIdentifier tableIdentifier;
    protected ExpectedResult expectedResult;
    protected boolean ignoreException;

    protected TableEnvironment tEnv;
    protected String rootPath;

    protected ResolvedCatalogTable resolvedTable =
            createResolvedTable(
                    Collections.emptyMap(),
                    RowType.of(new IntType(), new VarCharType()),
                    Collections.emptyList(),
                    Collections.emptyList());

    protected void prepareEnv(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean ignoreException,
            String manualCause,
            ExpectedResult expectedResult) {
        this.tableIdentifier = ObjectIdentifier.of(CURRENT_CATALOG, CURRENT_DATABASE, tableName);
        this.ignoreException = ignoreException;
        this.expectedResult = expectedResult;

        if (executionMode == RuntimeExecutionMode.STREAMING) {
            tEnv = tableEnvironmentBuilder().streamingMode().checkpointIntervalMs(100).build();
        } else {
            tEnv = tableEnvironmentBuilder().batchMode().build();
        }
        rootPath = getTempDirPath();

        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH ('type' = 'paimon', 'warehouse' = '%s')",
                        CURRENT_CATALOG, rootPath));
        tEnv.useCatalog(CURRENT_CATALOG);
    }

    /** Test class implements "private static List'<'Arguments> data()" to supply test data. */
    @ParameterizedTest
    @MethodSource("data")
    public void test(
            RuntimeExecutionMode executionMode,
            String tableName,
            boolean ignoreException,
            String manualCause,
            ExpectedResult expectedResult) {
        prepareEnv(executionMode, tableName, ignoreException, manualCause, expectedResult);
        testCore();
    }

    protected abstract void testCore();

    protected static ResolvedCatalogTable createResolvedTable(
            Map<String, String> options,
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys) {
        List<String> fieldNames = rowType.getFieldNames();
        List<DataType> fieldDataTypes =
                rowType.getChildren().stream()
                        .map(TypeConversions::fromLogicalToDataType)
                        .collect(Collectors.toList());
        List<Column> resolvedColumns =
                IntStream.range(0, fieldNames.size())
                        .mapToObj(i -> Column.physical(fieldNames.get(i), fieldDataTypes.get(i)))
                        .collect(Collectors.toList());
        return createResolvedTable(options, resolvedColumns, partitionKeys, primaryKeys);
    }

    protected static ResolvedCatalogTable createResolvedTable(
            Map<String, String> options,
            List<Column> resolvedColumns,
            List<String> partitionKeys,
            List<String> primaryKeys) {
        UniqueConstraint constraint =
                primaryKeys.isEmpty() ? null : UniqueConstraint.primaryKey("pk", primaryKeys);
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(resolvedColumns, Collections.emptyList(), constraint);
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "a comment",
                        partitionKeys,
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    protected void deleteTablePath() {
        FileUtils.deleteQuietly(Paths.get(rootPath, relativeTablePath(tableIdentifier)).toFile());
    }

    protected String relativeTablePath(ObjectIdentifier tableIdentifier) {
        return String.format(
                "%s.db/%s", tableIdentifier.getDatabaseName(), tableIdentifier.getObjectName());
    }

    /** Expected result wrapper. */
    protected static class ExpectedResult {
        protected boolean success;
        protected List<Row> expectedRecords;
        protected boolean failureHasCause;
        protected Class<? extends Throwable> expectedType;
        protected String expectedMessage;

        ExpectedResult success(boolean success) {
            this.success = success;
            return this;
        }

        ExpectedResult expectedRecords(List<Row> expectedRecords) {
            this.expectedRecords = expectedRecords;
            return this;
        }

        ExpectedResult failureHasCause(boolean failureHasCause) {
            this.failureHasCause = failureHasCause;
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
                    + ", expectedRecords="
                    + expectedRecords
                    + ", failureHasCause="
                    + failureHasCause
                    + ", expectedType="
                    + expectedType
                    + ", expectedMessage='"
                    + expectedMessage
                    + '\''
                    + '}';
        }
    }
}
