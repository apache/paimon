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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.DeleteAction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** The IT Case for the cdc source. */
public class CDCSourceITCase extends CDCSourceITCaseBase {

    private final TableId table1 = TableId.tableId("test", "table1");
    private final TableId table2 = TableId.tableId("test2", "table2");

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("varchar_value", DataTypes.VARCHAR(20))
                    .column("char_value", DataTypes.CHAR(20))
                    .column("tiny_int_value", DataTypes.TINYINT())
                    .column("small_int_value", DataTypes.SMALLINT())
                    .column("big_int_value", DataTypes.BIGINT())
                    .column("float_value", DataTypes.FLOAT())
                    .column("double_value", DataTypes.DOUBLE())
                    .column("boolean_value", DataTypes.BOOLEAN())
                    .column("decimal_value", DataTypes.DECIMAL(10, 2))
                    .primaryKey("id")
                    .option("parquet.use-native", "false")
                    .build();

    private static final String[] DATA_SQLS =
            new String[] {
                "(1, 'a', 'a', CAST(1 AS TINYINT), CAST(1 AS SMALLINT), CAST(1 AS BIGINT), 1.0, 1.0, true, CAST('123.45' AS DECIMAL(10, 2)))",
                "(2, 'b', 'b', CAST(2 AS TINYINT), CAST(2 AS SMALLINT), CAST(2 AS BIGINT), 2.0, 2.0, true, CAST('123.45' AS DECIMAL(10, 2)))",
                "(1, 'c', 'c', CAST(3 AS TINYINT), CAST(3 AS SMALLINT), CAST(3 AS BIGINT), 3.0, 3.0, false)",
                "(1, 'd', 'd', CAST(4 AS TINYINT), CAST(4 AS SMALLINT), CAST(4 AS BIGINT), 4.0, 4.0, false)",
            };

    private static final String[] DATA_VALUES =
            new String[] {
                "id=1;varchar_value=a;char_value=a                   ;tiny_int_value=1;small_int_value=1;big_int_value=1;float_value=1.0;double_value=1.0;boolean_value=true;decimal_value=123.45",
                "id=2;varchar_value=b;char_value=b                   ;tiny_int_value=2;small_int_value=2;big_int_value=2;float_value=2.0;double_value=2.0;boolean_value=true;decimal_value=123.45",
                "id=1;varchar_value=c;char_value=c                   ;tiny_int_value=3;small_int_value=3;big_int_value=3;float_value=3.0;double_value=3.0;boolean_value=false",
                "id=2;varchar_value=b;char_value=b                   ;tiny_int_value=2;small_int_value=2;big_int_value=2;float_value=2.0;double_value=2.0;boolean_value=true",
                "id=1;varchar_value=d;char_value=d                   ;tiny_int_value=4;small_int_value=4;big_int_value=4;float_value=4.0;double_value=4.0;boolean_value=false",
            };

    @AfterEach
    @Override
    public void afterEach() throws Exception {
        super.afterEach();
        catalog.dropDatabase(table1.getSchemaName(), true, true);
        catalog.dropDatabase(table2.getSchemaName(), true, true);
    }

    @Test
    public void test() throws Exception {
        catalog.createDatabase(table1.getSchemaName(), false);
        catalog.createTable(
                Identifier.create(table1.getSchemaName(), table1.getTableName()), SCHEMA, false);

        Configuration sinkConfig = new Configuration();
        sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
        sinkConfig.set(ValuesDataSinkOptions.PRINT_ENABLED, false);

        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, 1);

        // Execute the pipeline
        jobClient = createAndExecutePipeline(env, sinkConfig, pipelineConfig);

        insertInto(table1, DATA_SQLS[0], DATA_SQLS[1]);
        waitUtilExpectedResult(table1, DATA_VALUES[0], DATA_VALUES[1]);

        catalog.createDatabase(table2.getSchemaName(), false);
        catalog.createTable(
                Identifier.create(table2.getSchemaName(), table2.getTableName()), SCHEMA, false);

        insertInto(table2, DATA_SQLS[0], DATA_SQLS[1]);
        waitUtilExpectedResult(table2, DATA_VALUES[0], DATA_VALUES[1]);

        DeleteAction action =
                new DeleteAction(
                        table1.getSchemaName(),
                        table1.getTableName(),
                        "id = 1",
                        getCatalogOptions().toMap());

        action.run();
        waitUtilExpectedResult(table1, DATA_VALUES[1]);

        catalog.alterTable(
                Identifier.create(table1.getSchemaName(), table1.getTableName()),
                SchemaChange.dropColumn("decimal_value"),
                false);
        insertInto(table1, DATA_SQLS[2]);
        waitUtilExpectedResult(table1, DATA_VALUES[2], DATA_VALUES[3]);
    }

    private void waitUtilExpectedResult(TableId tableId, String... expectedResultArray)
            throws InterruptedException {
        List<String> expectedResult =
                Arrays.stream(expectedResultArray)
                        .map(x -> String.format("%s:%s", tableId, x))
                        .sorted()
                        .collect(Collectors.toList());
        waitUtilExpectedResultInternal(tableId, expectedResult);
    }

    @Override
    protected List<String> getActualResult(TableId tableId) {
        try {
            return ValuesDatabase.getResults(tableId);
        } catch (NullPointerException e) {
            return Collections.singletonList("Uninitialized result");
        }
    }
}
