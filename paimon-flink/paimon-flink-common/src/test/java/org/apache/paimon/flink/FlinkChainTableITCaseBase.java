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

import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Base class for chain table Flink IT cases providing shared helper methods. */
public abstract class FlinkChainTableITCaseBase extends CatalogITCaseBase {

    protected List<String> collectResult(String query) throws Exception {
        List<String> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
            while (it.hasNext()) {
                result.add(it.next().toString());
            }
        }
        return result;
    }

    protected void createChainTable(String tableName) {
        sql(
                "CREATE TABLE %s ("
                        + "  t1 BIGINT,"
                        + "  t2 BIGINT,"
                        + "  t3 STRING,"
                        + "  dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "  'primary-key' = 'dt,t1',"
                        + "  'bucket-key' = 't1',"
                        + "  'bucket' = '2',"
                        + "  'sequence.field' = 't2',"
                        + "  'merge-engine' = 'deduplicate',"
                        + "  'chain-table.enabled' = 'true',"
                        + "  'partition.timestamp-pattern' = '$dt',"
                        + "  'partition.timestamp-formatter' = 'yyyyMMdd'"
                        + ")",
                tableName);
    }

    protected void setupChainTableBranches(String tableName) {
        sql("CALL sys.create_branch('%s.%s', 'snapshot')", tEnv.getCurrentDatabase(), tableName);
        sql("CALL sys.create_branch('%s.%s', 'delta')", tEnv.getCurrentDatabase(), tableName);

        sql(
                "ALTER TABLE %s SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
        sql(
                "ALTER TABLE `%s$branch_snapshot` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
        sql(
                "ALTER TABLE `%s$branch_delta` SET ("
                        + "  'scan.fallback-snapshot-branch' = 'snapshot',"
                        + "  'scan.fallback-delta-branch' = 'delta'"
                        + ")",
                tableName);
    }

    /** Write Row data (with RowKind) to a specific branch using DataStream API. */
    protected void writeChangelogToBranch(String db, String tableName, String branch, Row... rows)
            throws Exception {
        FileStoreTable table = paimonTable(tableName + "$branch_" + branch);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(100)
                        .parallelism(1)
                        .build();

        DataStream<Row> stream = env.fromCollection(Arrays.asList(rows));

        new FlinkSinkBuilder(table)
                .forRow(
                        stream,
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.BIGINT()),
                                DataTypes.FIELD("seq", DataTypes.BIGINT()),
                                DataTypes.FIELD("v", DataTypes.STRING()),
                                DataTypes.FIELD("dt", DataTypes.STRING())))
                .build();
        env.execute();
    }

    /**
     * Write Row data (with RowKind and a group partition column) to a specific branch using
     * DataStream API.
     */
    protected void writeChangelogToBranchWithRegion(
            String db, String tableName, String branch, Row... rows) throws Exception {
        FileStoreTable table = paimonTable(tableName + "$branch_" + branch);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .checkpointIntervalMs(100)
                        .parallelism(1)
                        .build();

        DataStream<Row> stream = env.fromCollection(Arrays.asList(rows));

        new FlinkSinkBuilder(table)
                .forRow(
                        stream,
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.BIGINT()),
                                DataTypes.FIELD("seq", DataTypes.BIGINT()),
                                DataTypes.FIELD("v", DataTypes.STRING()),
                                DataTypes.FIELD("region", DataTypes.STRING()),
                                DataTypes.FIELD("dt", DataTypes.STRING())))
                .build();
        env.execute();
    }

    /**
     * Collects {@code n} rows from a streaming iterator using the project-standard {@link
     * BlockingIterator}.
     */
    protected List<String> collectRows(CloseableIterator<Row> it, int n) throws Exception {
        return BlockingIterator.of(it).collect(n, 30, TimeUnit.SECONDS).stream()
                .map(Row::toString)
                .collect(Collectors.toList());
    }

    /**
     * Polls the given table until it contains at least {@code minRows} rows. Used instead of
     * fixed-duration Thread.sleep to avoid flaky tests on slow CI.
     */
    protected void waitForRowCount(String tableName, int minRows) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        int count = 0;
        while (System.currentTimeMillis() < deadline) {
            List<Row> rows = sql("SELECT * FROM " + tableName);
            count = rows.size();
            if (count >= minRows) {
                return;
            }
            Thread.sleep(1000);
        }
        throw new AssertionError(
                "Timed out waiting for " + minRows + " rows in " + tableName + ", got " + count);
    }

    /** Helper method: poll a query until the result matches the expected condition or timeout. */
    protected void waitForQueryResult(
            String query, java.util.function.Predicate<List<String>> condition) throws Exception {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 30000L) {
            List<String> results = collectResult(query);
            if (condition.test(results)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new RuntimeException("Timed out waiting for query result: " + query);
    }

    /** Helper method: wait for job to reach target status. */
    protected void waitForJobRunning(JobClient jobClient) throws Exception {
        long startTime = System.currentTimeMillis();
        JobStatus currentStatus = null;
        while (System.currentTimeMillis() - startTime < (long) 30000) {
            CompletableFuture<JobStatus> statusFuture = jobClient.getJobStatus();
            currentStatus = statusFuture.get();

            if (currentStatus == JobStatus.RUNNING) {
                return;
            }

            if (currentStatus.isGloballyTerminalState()) {
                throw new RuntimeException(
                        "Job terminated unexpectedly with status: " + currentStatus);
            }

            Thread.sleep(500);
        }
        throw new RuntimeException(
                "Timed out waiting for job status running. Current status: " + currentStatus);
    }
}
