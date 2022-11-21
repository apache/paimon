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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.CoreOptions.LogStartupMode;
import org.apache.flink.table.store.CoreOptions.MergeEngine;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Optional;

import static org.apache.flink.table.store.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.flink.table.store.CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.flink.table.store.CoreOptions.ChangelogProducer.FULL_COMPACTION;
import static org.apache.flink.table.store.CoreOptions.LOG_SCAN;
import static org.apache.flink.table.store.CoreOptions.MERGE_ENGINE;
import static org.apache.flink.table.store.connector.FlinkConnectorOptions.COMPACTION_MANUAL_TRIGGERED;

/** Source builder to build a Flink {@link Source}. */
public class FlinkSourceBuilder {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final Configuration conf;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private int[][] projectedFields;
    @Nullable private Predicate predicate;
    @Nullable private LogSourceProvider logSourceProvider;
    @Nullable private Integer parallelism;
    @Nullable private Long limit;
    @Nullable private WatermarkStrategy<RowData> watermarkStrategy;

    public FlinkSourceBuilder(ObjectIdentifier tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.conf = Configuration.fromMap(table.schema().options());
    }

    public FlinkSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public FlinkSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public FlinkSourceBuilder withProjection(int[][] projectedFields) {
        this.projectedFields = projectedFields;
        return this;
    }

    public FlinkSourceBuilder withPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public FlinkSourceBuilder withLimit(@Nullable Long limit) {
        this.limit = limit;
        return this;
    }

    public FlinkSourceBuilder withLogSourceProvider(LogSourceProvider logSourceProvider) {
        this.logSourceProvider = logSourceProvider;
        return this;
    }

    public FlinkSourceBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSourceBuilder withWatermarkStrategy(
            @Nullable WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
        return this;
    }

    private long discoveryIntervalMills() {
        return conf.get(CONTINUOUS_DISCOVERY_INTERVAL).toMillis();
    }

    private FileStoreSource buildFileSource(boolean isContinuous) {
        return new FileStoreSource(
                table, isContinuous, discoveryIntervalMills(), projectedFields, predicate, limit);
    }

    private Source<RowData, ?, ?> buildSource() {
        if (isContinuous) {
            // TODO move validation to a dedicated method
            MergeEngine mergeEngine = conf.get(MERGE_ENGINE);
            HashMap<MergeEngine, String> mergeEngineDesc =
                    new HashMap<MergeEngine, String>() {
                        {
                            put(MergeEngine.PARTIAL_UPDATE, "Partial update");
                            put(MergeEngine.AGGREGATE, "Pre-aggregate");
                        }
                    };
            if (table.schema().primaryKeys().size() > 0
                    && mergeEngineDesc.containsKey(mergeEngine)
                    && conf.get(CHANGELOG_PRODUCER) != FULL_COMPACTION) {
                throw new ValidationException(
                        mergeEngineDesc.get(mergeEngine)
                                + " continuous reading is not supported. "
                                + "You can use full compaction changelog producer to support streaming reading.");
            }

            LogStartupMode startupMode = conf.get(LOG_SCAN);
            if (logSourceProvider == null) {
                return buildFileSource(true);
            } else {
                if (startupMode != LogStartupMode.FULL) {
                    return logSourceProvider.createSource(null);
                }
                return HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                buildFileSource(false))
                        .addSource(
                                new LogHybridSourceFactory(logSourceProvider),
                                Boundedness.CONTINUOUS_UNBOUNDED)
                        .build();
            }
        } else {
            return buildFileSource(false);
        }
    }

    public DataStreamSource<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        RowType rowType = table.schema().logicalRowType();
        LogicalType produceType =
                Optional.ofNullable(projectedFields)
                        .map(Projection::of)
                        .map(p -> p.project(rowType))
                        .orElse(rowType);
        DataStreamSource<RowData> dataStream =
                env.fromSource(
                        conf.get(COMPACTION_MANUAL_TRIGGERED)
                                ? new FileStoreEmptySource()
                                : buildSource(),
                        watermarkStrategy == null
                                ? WatermarkStrategy.noWatermarks()
                                : watermarkStrategy,
                        tableIdentifier.asSummaryString(),
                        InternalTypeInfo.of(produceType));
        if (parallelism != null) {
            dataStream.setParallelism(parallelism);
        }
        return dataStream;
    }
}
