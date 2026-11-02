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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/** End-to-end test of {@link FlinkSourceBuilder#buildSource()} embedded in a HybridSource. */
public class HybridSourceITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE hybrid_source (a INT, b STRING)",
                "CREATE TABLE hybrid_sink (a INT, b STRING)");
    }

    @Timeout(300)
    @Test
    public void testBoundedThenContinuousRead() throws Exception {
        batchSql("INSERT INTO hybrid_source VALUES (1, 'first'), (2, 'second')");

        FileStoreTable table = paimonTable("hybrid_source");

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(500)
                        .build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        streamTableEnv.registerCatalog("PAIMON_STREAM", sEnv.getCatalog("PAIMON").get());
        streamTableEnv.executeSql("USE CATALOG PAIMON_STREAM");

        Source<RowData, ?, ?> bounded =
                new FlinkSourceBuilder(table).sourceBounded(true).buildSource();
        // The continuous segment (latest-full) would re-read the bounded segment's data, so it
        // filters a <= 2: rows 1-2 can then only be delivered by the bounded segment.
        Predicate batch2Only = new PredicateBuilder(table.rowType()).greaterThan(0, 2);
        HybridSource<RowData> hybridSource =
                HybridSource.builder(bounded)
                        .addSource(
                                context ->
                                        new FlinkSourceBuilder(table)
                                                .predicate(batch2Only)
                                                .sourceBounded(false)
                                                .buildSource(),
                                Boundedness.CONTINUOUS_UNBOUNDED)
                        .build();

        DataType dataType = fromLogicalToDataType(toLogicalType(table.rowType()));
        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(
                        dataType.getChildren().toArray(new DataType[0]));
        DataStream<Row> rows =
                env.fromSource(
                                hybridSource,
                                WatermarkStrategy.noWatermarks(),
                                "HybridSource",
                                InternalTypeInfo.of(toLogicalType(table.rowType())))
                        .map((MapFunction<RowData, Row>) converter::toExternal)
                        .returns(ExternalTypeInfo.of(dataType));

        streamTableEnv.createTemporaryView("S", streamTableEnv.fromDataStream(rows).as("a", "b"));
        TableResult result = streamTableEnv.executeSql("INSERT INTO hybrid_sink SELECT * FROM S");
        JobClient client = result.getJobClient().get();

        try {
            // written after the job is submitted; the continuous segment sees it either in its
            // starting snapshot or incrementally, so both segments are still verified
            batchSql("INSERT INTO hybrid_source VALUES (3, 'third'), (4, 'fourth')");

            // DISTINCT tolerates batch2 being read by both segments, but rows 1-2 are filtered
            // out of the continuous segment and thus prove the bounded segment ran
            sqlAssertWithRetry(
                    "SELECT DISTINCT * FROM hybrid_sink",
                    assertion ->
                            assertion.containsExactlyInAnyOrder(
                                    Row.of(1, "first"),
                                    Row.of(2, "second"),
                                    Row.of(3, "third"),
                                    Row.of(4, "fourth")));
        } finally {
            try {
                client.cancel().get(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // best-effort cleanup: the job may already be terminated
            }
        }
    }
}
