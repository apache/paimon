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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.operator.MultiTablesBatchCompactorSourceFunction;
import org.apache.paimon.flink.source.operator.MultiTablesStreamingCompactorSourceFunction;
import org.apache.paimon.table.system.BucketsMultiTable;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.regex.Pattern;

/**
 * source builder to build a Flink compactor source for multi-tables. This is for dedicated
 * compactor jobs.
 */
public class MultiTablesCompactorSourceBuilder {
    private final Catalog.Loader catalogLoader;
    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;
    private final long monitorInterval;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;

    public MultiTablesCompactorSourceBuilder(
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

    public MultiTablesCompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public MultiTablesCompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }
        RowType produceType = BucketsMultiTable.getRowType();
        if (isContinuous) {
            return MultiTablesStreamingCompactorSourceFunction.buildSource(
                    env,
                    "MultiTables-StreamingCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        } else {
            return MultiTablesBatchCompactorSourceFunction.buildSource(
                    env,
                    "MultiTables-BatchCompactorSource",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    catalogLoader,
                    includingPattern,
                    excludingPattern,
                    databasePattern,
                    monitorInterval);
        }
    }
}
