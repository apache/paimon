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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.predicate.PartitionPredicateVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;

/** Table compact action for Flink. */
public class CompactAction extends TableActionBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactAction.class);

    private List<Map<String, String>> partitions;

    private String whereSql;

    @Nullable private Duration partitionIdleTime = null;

    public CompactAction(String warehouse, String database, String tableName) {
        this(warehouse, database, tableName, Collections.emptyMap(), Collections.emptyMap());
    }

    public CompactAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(warehouse, database, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                            table.getClass().getName()));
        }
        HashMap<String, String> dynamicOptions = new HashMap<>(tableConf);
        dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        table = table.copy(dynamicOptions);
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public CompactAction withPartitions(List<Map<String, String>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public CompactAction withWhereSql(String whereSql) {
        this.whereSql = whereSql;
        return this;
    }

    public CompactAction withPartitionIdleTime(@Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
        return this;
    }

    @Override
    public void build() throws Exception {
        ReadableConfig conf = env.getConfiguration();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        switch (fileStoreTable.bucketMode()) {
            case BUCKET_UNAWARE:
                {
                    buildForUnawareBucketCompaction(env, fileStoreTable, isStreaming);
                    break;
                }
            case HASH_FIXED:
            case HASH_DYNAMIC:
            default:
                {
                    buildForTraditionalCompaction(env, fileStoreTable, isStreaming);
                }
        }
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        if (isStreaming) {
            // for completely asynchronous compaction
            HashMap<String, String> dynamicOptions =
                    new HashMap<String, String>() {
                        {
                            put(CoreOptions.NUM_SORTED_RUNS_STOP_TRIGGER.key(), "2147483647");
                            put(CoreOptions.SORT_SPILL_THRESHOLD.key(), "10");
                            put(CoreOptions.LOOKUP_WAIT.key(), "false");
                        }
                    };
            table = table.copy(dynamicOptions);
        }
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(identifier.getFullName(), table);
        CompactorSinkBuilder sinkBuilder = new CompactorSinkBuilder(table);

        sourceBuilder.withPartitionPredicate(getPredicate());
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(isStreaming)
                        .withPartitionIdleTime(partitionIdleTime)
                        .build();
        sinkBuilder.withInput(source).build();
    }

    private void buildForUnawareBucketCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming)
            throws Exception {
        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
                new UnawareBucketCompactionTopoBuilder(env, identifier.getFullName(), table);

        unawareBucketCompactionTopoBuilder.withPartitionPredicate(getPredicate());
        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.withPartitionIdleTime(partitionIdleTime);
        unawareBucketCompactionTopoBuilder.build();
    }

    protected Predicate getPredicate() throws Exception {
        Preconditions.checkArgument(
                partitions == null || whereSql == null,
                "partitions and where cannot be used together.");
        Predicate predicate = null;
        if (partitions != null) {
            predicate =
                    PredicateBuilder.or(
                            partitions.stream()
                                    .map(
                                            p ->
                                                    createPartitionPredicate(
                                                            p,
                                                            table.rowType(),
                                                            ((FileStoreTable) table)
                                                                    .coreOptions()
                                                                    .partitionDefaultName()))
                                    .toArray(Predicate[]::new));
        } else if (whereSql != null) {
            SimpleSqlPredicateConvertor simpleSqlPredicateConvertor =
                    new SimpleSqlPredicateConvertor(table.rowType());
            predicate = simpleSqlPredicateConvertor.convertSqlToPredicate(whereSql);
        }

        // Check whether predicate contain non partition key.
        if (predicate != null) {
            LOGGER.info("the partition predicate of compaction is {}", predicate);
            PartitionPredicateVisitor partitionPredicateVisitor =
                    new PartitionPredicateVisitor(table.partitionKeys());
            Preconditions.checkArgument(
                    predicate.visit(partitionPredicateVisitor),
                    "Only partition key can be specialized in compaction action.");
        }

        return predicate;
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Compact job");
    }
}
