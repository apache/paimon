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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.connector.files.shaded.org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.sink.BucketStreamPartitioner;
import org.apache.flink.table.store.connector.sink.StoreSink;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSinkTranslator;
import org.apache.flink.table.store.connector.source.FileStoreSource;
import org.apache.flink.table.store.connector.source.LogHybridSourceFactory;
import org.apache.flink.table.store.connector.source.StaticFileStoreSplitEnumerator;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreImpl;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.log.LogOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.store.utils.TypeUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.CONTINUOUS_DISCOVERY_INTERVAL;
import static org.apache.flink.table.store.log.LogOptions.LOG_PREFIX;
import static org.apache.flink.table.store.log.LogOptions.SCAN;

/** A table store api to create source and sink. */
@Experimental
public class TableStore {

    private final Configuration options;

    /** commit user, default uuid. */
    private final String user;

    /** partition keys, default no partition. */
    private final int[] partitions;

    /** primary keys, default no key. */
    private final int[] primaryKeys;

    /** physical row type. */
    private final RowType type;

    private final ObjectIdentifier tableIdentifier;

    private TableStore(
            Configuration options,
            ObjectIdentifier tableIdentifier,
            RowType type,
            int[] partitions,
            int[] primaryKeys,
            String user) {
        this.options = options;
        this.tableIdentifier = tableIdentifier;
        this.type = type;
        this.partitions = partitions;
        this.primaryKeys = primaryKeys;
        this.user = user;
    }

    public TableStoreBuilder toBuilder() {
        return new TableStoreBuilder()
                .withConfiguration(options)
                .withTableIdentifier(tableIdentifier)
                .withSchema(type)
                .withPrimaryKeys(primaryKeys())
                .withPartitionKeys(partitionKeys())
                .withUser(user);
    }

    public boolean partitioned() {
        return partitions.length > 0;
    }

    public boolean valueCountMode() {
        return primaryKeys.length == 0;
    }

    public List<String> fieldNames() {
        return type.getFieldNames();
    }

    public List<String> partitionKeys() {
        RowType partitionType = TypeUtils.project(type, partitions);
        return partitionType.getFieldNames();
    }

    @VisibleForTesting
    List<String> primaryKeys() {
        RowType primaryKeyType = TypeUtils.project(type, primaryKeys);
        return primaryKeyType.getFieldNames();
    }

    public Configuration logOptions() {
        return new DelegatingConfiguration(options, LOG_PREFIX);
    }

    public SourceBuilder sourceBuilder() {
        return new SourceBuilder();
    }

    public SinkBuilder sinkBuilder() {
        return new SinkBuilder();
    }

    private FileStore buildFileStore() {
        RowType partitionType = TypeUtils.project(type, partitions);
        RowType keyType;
        RowType valueType;
        MergeFunction mergeFunction;
        if (primaryKeys.length == 0) {
            keyType = type;
            valueType = RowType.of(new BigIntType(false));
            mergeFunction = new ValueCountMergeFunction();
        } else {
            List<RowType.RowField> fields = TypeUtils.project(type, primaryKeys).getFields();
            // add _KEY_ prefix to avoid conflict with value
            keyType =
                    new RowType(
                            fields.stream()
                                    .map(
                                            f ->
                                                    new RowType.RowField(
                                                            "_KEY_" + f.getName(),
                                                            f.getType(),
                                                            f.getDescription().orElse(null)))
                                    .collect(Collectors.toList()));
            valueType = type;
            mergeFunction = new DeduplicateMergeFunction();
        }
        return new FileStoreImpl(
                tableIdentifier, options, user, partitionType, keyType, valueType, mergeFunction);
    }

    // --------------------------------------------------------------------------------------------

    /** A builder for constructing a immutable {@link TableStore}. */
    public static final class TableStoreBuilder {

        private Configuration options;
        private ObjectIdentifier tableIdentifier;
        private RowType type;

        private List<String> primaryKeys = new ArrayList<>();
        private List<String> partitionKeys = new ArrayList<>();
        private String user = UUID.randomUUID().toString();

        public TableStoreBuilder withConfiguration(Configuration options) {
            this.options = options;
            return this;
        }

        public TableStoreBuilder withTableIdentifier(ObjectIdentifier tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        public TableStoreBuilder withSchema(RowType type) {
            this.type = type;
            return this;
        }

        public TableStoreBuilder withPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public TableStoreBuilder withPartitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public TableStoreBuilder withUser(String user) {
            this.user = user;
            return this;
        }

        public TableStore build() {
            List<int[]> indices = adjustAndValidate();
            return new TableStore(
                    options, tableIdentifier, type, indices.get(0), indices.get(1), user);
        }

        private List<int[]> adjustAndValidate() {
            if (primaryKeys.size() > 0 && partitionKeys.size() > 0) {
                // primary keys must contain all partition keys if table is partitioned
                Preconditions.checkState(
                        new HashSet<>(primaryKeys).containsAll(partitionKeys),
                        String.format(
                                "Primary key constraint %s should include all partition fields %s",
                                primaryKeys, partitionKeys));
            }
            Set<String> partFilter = new HashSet<>(partitionKeys);
            int[] pks =
                    primaryKeys.stream()
                            .filter(pk -> !partFilter.contains(pk))
                            .mapToInt(type.getFieldNames()::indexOf)
                            .toArray();
            if (pks.length == 0 && primaryKeys.size() > 0) {
                throw new TableException(
                        String.format(
                                "Primary key constraint %s should not be same with partition fields %s,"
                                        + " this will result in only one record in a partition",
                                primaryKeys, partitionKeys));
            }
            // adjust index according to physical row type
            return Arrays.asList(
                    partitionKeys.stream().mapToInt(type.getFieldNames()::indexOf).toArray(), pks);
        }
    }

    /** Source builder to build a flink {@link Source}. */
    public class SourceBuilder {

        private boolean isContinuous = false;

        private boolean isHybrid = true;

        @Nullable private int[][] projectedFields;

        @Nullable private Predicate partitionPredicate;

        @Nullable private Predicate fieldPredicate;

        @Nullable private LogSourceProvider logSourceProvider;

        public SourceBuilder withProjection(int[][] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public SourceBuilder withPartitionPredicate(Predicate partitionPredicate) {
            this.partitionPredicate = partitionPredicate;
            return this;
        }

        public SourceBuilder withFieldPredicate(Predicate fieldPredicate) {
            this.fieldPredicate = fieldPredicate;
            return this;
        }

        public SourceBuilder withContinuousMode(boolean isContinuous) {
            this.isContinuous = isContinuous;
            return this;
        }

        public SourceBuilder withHybridMode(boolean isHybrid) {
            this.isHybrid = isHybrid;
            return this;
        }

        public SourceBuilder withLogSourceProvider(LogSourceProvider logSourceProvider) {
            this.logSourceProvider = logSourceProvider;
            return this;
        }

        private long discoveryIntervalMills() {
            return options.get(CONTINUOUS_DISCOVERY_INTERVAL).toMillis();
        }

        private FileStoreSource buildFileSource(boolean isContinuous) {
            FileStore fileStore = buildFileStore();

            boolean latestContinuous = false;
            if (isContinuous) {
                LogStartupMode startupMode = logOptions().get(SCAN);
                latestContinuous = startupMode == LogStartupMode.LATEST;
            }
            return new FileStoreSource(
                    fileStore,
                    primaryKeys.length == 0,
                    isContinuous,
                    discoveryIntervalMills(),
                    latestContinuous,
                    projectedFields,
                    partitionPredicate,
                    fieldPredicate);
        }

        public Source<RowData, ?, ?> build() {
            if (isContinuous) {
                if (logSourceProvider == null) {
                    return buildFileSource(true);
                }

                // TODO project log source

                if (isHybrid) {
                    return HybridSource.<RowData, StaticFileStoreSplitEnumerator>builder(
                                    buildFileSource(false))
                            .addSource(
                                    new LogHybridSourceFactory(logSourceProvider),
                                    Boundedness.CONTINUOUS_UNBOUNDED)
                            .build();
                } else {
                    return logSourceProvider.createSource(null);
                }
            } else {
                return buildFileSource(false);
            }
        }

        public DataStreamSource<RowData> build(StreamExecutionEnvironment env) {
            return env.fromSource(
                    build(),
                    WatermarkStrategy.noWatermarks(),
                    tableIdentifier.asSummaryString(),
                    InternalTypeInfo.of(type));
        }
    }

    /** Sink builder to build a flink sink from input. */
    public class SinkBuilder {

        private DataStream<RowData> input;

        @Nullable private CatalogLock.Factory lockFactory;

        @Nullable private Map<String, String> overwritePartition;

        @Nullable private LogSinkProvider logSinkProvider;

        public SinkBuilder withInput(DataStream<RowData> input) {
            this.input = input;
            return this;
        }

        public SinkBuilder withLockFactory(CatalogLock.Factory lockFactory) {
            this.lockFactory = lockFactory;
            return this;
        }

        public SinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
            this.overwritePartition = overwritePartition;
            return this;
        }

        public SinkBuilder withLogSinkProvider(LogSinkProvider logSinkProvider) {
            this.logSinkProvider = logSinkProvider;
            return this;
        }

        public DataStreamSink<?> build() {
            FileStore fileStore = buildFileStore();
            int numBucket = options.get(BUCKET);

            BucketStreamPartitioner partitioner =
                    new BucketStreamPartitioner(numBucket, type, partitions, primaryKeys);
            DataStream<RowData> partitioned =
                    new DataStream<>(
                            input.getExecutionEnvironment(),
                            new PartitionTransformation<>(input.getTransformation(), partitioner));

            StoreSink<?, ?> sink =
                    new StoreSink<>(
                            tableIdentifier,
                            fileStore,
                            partitions,
                            primaryKeys,
                            numBucket,
                            lockFactory,
                            overwritePartition,
                            logSinkProvider);
            return GlobalCommittingSinkTranslator.translate(partitioned, sink);
        }
    }
}
