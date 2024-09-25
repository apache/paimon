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

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.operator.CombinedAwareBatchSourceFunction;
import org.apache.paimon.flink.source.operator.CombinedAwareStreamingSourceFunction;
import org.apache.paimon.flink.source.operator.CombinedUnawareBatchSourceFunction;
import org.apache.paimon.flink.source.operator.CombinedUnawareStreamingSourceFunction;
import org.apache.paimon.table.system.CompactBucketsTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * source builder to build a Flink compactor source for multi-tables. This is for dedicated
 * compactor jobs in combined mode.
 */
public class CombinedTableCompactorSourceBuilder {
    private final Catalog.Loader catalogLoader;
    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;
    private final long monitorInterval;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private Duration partitionIdleTime = null;

    public CombinedTableCompactorSourceBuilder(
            Catalog.Loader catalogLoader,
            Pattern databasePattern,
            Pattern includingPattern,
            Pattern excludingPattern,
            long monitorInterval) {
        this.catalogLoader = catalogLoader;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.databasePattern = databasePattern;
        this.monitorInterval = monitorInterval;
    }

    public CombinedTableCompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public CombinedTableCompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public CombinedTableCompactorSourceBuilder withPartitionIdleTime(
            @Nullable Duration partitionIdleTime) {
        this.partitionIdleTime = partitionIdleTime;
        return this;
    }

    public DataStream<RowData> buildAwareBucketTableSource() {
        Preconditions.checkArgument(env != null, "StreamExecutionEnvironment should not be null.");
        RowType produceType = CompactBucketsTable.getRowType();
        if (isContinuous) {
            return CombinedAwareStreamingSourceFunction.buildSource(
                    env,
                    "Combine-MultiBucketTables--StreamingCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        } else {
            return CombinedAwareBatchSourceFunction.buildSource(
                    env,
                    "Combine-MultiBucketTables-BatchCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    partitionIdleTime);
        }
    }

    public DataStream<MultiTableUnawareAppendCompactionTask> buildForUnawareBucketsTableSource() {
        Preconditions.checkArgument(env != null, "StreamExecutionEnvironment should not be null.");
        if (isContinuous) {
            return CombinedUnawareStreamingSourceFunction.buildSource(
                    env,
                    "Combined-UnawareBucketTables-StreamingCompactorSource",
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        } else {
            return CombinedUnawareBatchSourceFunction.buildSource(
                    env,
                    "Combined-UnawareBucketTables-BatchCompactorSource",
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    partitionIdleTime);
        }
    }
}
