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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.connector.TableStoreFactoryOptions;
import org.apache.flink.table.store.connector.sink.global.GlobalCommittingSinkTranslator;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.JsonSerdeUtil;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.table.FileStoreTable;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.table.store.connector.TableStoreFactoryOptions.OVERWRITE_RESCALE_BUCKET;
import static org.apache.flink.table.store.file.FileStoreOptions.BUCKET;

/** Sink builder to build a flink sink from input. */
public class FlinkSinkBuilder {

    private final ObjectIdentifier tableIdentifier;
    private final FileStoreTable table;
    private final Configuration conf;

    private boolean isContinuous = false;
    private DataStream<RowData> input;
    @Nullable private CatalogLock.Factory lockFactory;
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private LogSinkProvider logSinkProvider;
    @Nullable private Integer parallelism;

    public FlinkSinkBuilder(ObjectIdentifier tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.conf = Configuration.fromMap(table.schema().options());
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public FlinkSinkBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public FlinkSinkBuilder withLockFactory(CatalogLock.Factory lockFactory) {
        this.lockFactory = lockFactory;
        return this;
    }

    public FlinkSinkBuilder withOverwritePartition(Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkProvider(LogSinkProvider logSinkProvider) {
        this.logSinkProvider = logSinkProvider;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private Map<String, String> getCompactPartSpec() {
        String json = conf.get(TableStoreFactoryOptions.COMPACTION_PARTITION_SPEC);
        if (json == null) {
            return null;
        }
        return JsonSerdeUtil.fromJson(json, Map.class);
    }

    private boolean rescaleBucket() {
        return !isContinuous && conf.get(OVERWRITE_RESCALE_BUCKET) && overwritePartition != null;
    }

    public DataStreamSink<?> build() {
        BucketStreamPartitioner partitioner =
                new BucketStreamPartitioner(getLatestNumOfBucket(), table.schema());
        PartitionTransformation<RowData> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }

        StoreSink<?, ?> sink =
                new StoreSink<>(
                        tableIdentifier,
                        table,
                        conf.get(TableStoreFactoryOptions.COMPACTION_MANUAL_TRIGGERED),
                        getCompactPartSpec(),
                        lockFactory,
                        overwritePartition,
                        logSinkProvider);
        return GlobalCommittingSinkTranslator.translate(
                new DataStream<>(input.getExecutionEnvironment(), partitioned), sink);
    }

    private int getLatestNumOfBucket() {
        SchemaManager schemaManager = new SchemaManager(FileStoreOptions.path(conf));
        SnapshotManager snapshotManager = table.snapshotManager();
        int currentBucketNum = conf.get(BUCKET);
        try {
            Long id = snapshotManager.findLatest();
            if (id != null) {
                Snapshot latestSnapshot = snapshotManager.snapshot(id);
                int bucketNumFromSnapshot =
                        getBucketNum(schemaManager.schema(latestSnapshot.schemaId()));
                if (rescaleBucket()) {
                    // special handling for managed table, because alter table does not update
                    // schema
                    Schema latestSchema = schemaManager.latest().get();
                    if (currentBucketNum != getBucketNum(latestSchema)) {
                        throw new UnsupportedOperationException(
                                "Rescale bucket overwrite is unsupported for Flink's managed table.");
                    }
                } else if (bucketNumFromSnapshot != currentBucketNum) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Try to write table with a new bucket num %d, but the previous bucket num is %d. "
                                            + "Please switch to batch mode, enable 'overwrite.rescale-bucket' and "
                                            + "perform INSERT OVERWRITE to rescale current data layout first.",
                                    currentBucketNum, bucketNumFromSnapshot));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return currentBucketNum;
    }

    private int getBucketNum(Schema schema) {
        return Integer.parseInt(
                schema.options().getOrDefault(BUCKET.key(), BUCKET.defaultValue().toString()));
    }
}
