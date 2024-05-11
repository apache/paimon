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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.utils.SingleOutputStreamOperatorUtils;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/**
 * Builder for CDC {@link FlinkWriteSink} when syncing the whole database into one Paimon database.
 * Each database table will be written into a separate Paimon table.
 *
 * <p>This builder will create a separate sink for each Paimon sink table. Thus this implementation
 * is not very efficient in resource saving.
 *
 * <p>For newly added tables, this builder will create a multiplexed Paimon sink to handle all
 * tables added during runtime. Note that the topology of the Flink job is likely to change when
 * there is newly added table and the job resume from a given savepoint.
 *
 * @param <T> CDC change event type
 */
public class FlinkCdcSyncDatabaseSinkBuilder<T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    private List<FileStoreTable> tables = new ArrayList<>();

    @Nullable private Integer parallelism;
    private double committerCpu;
    @Nullable private MemorySize committerMemory;
    private boolean commitChaining;

    // Paimon catalog used to check and create tables. There will be two
    //     places where this catalog is used. 1) in processing function,
    //     it will check newly added tables and create the corresponding
    //     Paimon tables. 2) in multiplex sink where it is used to
    //     initialize different writers to multiple tables.
    private Catalog.Loader catalogLoader;
    // database to sync, currently only support single database
    private String database;
    private MultiTablesSinkMode mode;

    public FlinkCdcSyncDatabaseSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withParserFactory(
            EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTables(List<FileStoreTable> tables) {
        this.tables = tables;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTableOptions(Map<String, String> options) {
        return withTableOptions(Options.fromMap(options));
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withTableOptions(Options options) {
        this.parallelism = options.get(FlinkConnectorOptions.SINK_PARALLELISM);
        this.committerCpu = options.get(FlinkConnectorOptions.SINK_COMMITTER_CPU);
        this.committerMemory = options.get(FlinkConnectorOptions.SINK_COMMITTER_MEMORY);
        this.commitChaining = options.get(FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING);
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withDatabase(String database) {
        this.database = database;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withCatalogLoader(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public FlinkCdcSyncDatabaseSinkBuilder<T> withMode(MultiTablesSinkMode mode) {
        this.mode = mode;
        return this;
    }

    public void build() {
        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(parserFactory);
        Preconditions.checkNotNull(database);
        Preconditions.checkNotNull(catalogLoader);

        if (mode == COMBINED) {
            buildCombinedCdcSink();
        } else {
            buildDividedCdcSink();
        }
    }

    private void buildCombinedCdcSink() {
        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(
                                new CdcDynamicTableParsingProcessFunction<>(
                                        database, catalogLoader, parserFactory))
                        .name("Side Output")
                        .setParallelism(input.getParallelism());

        // for newly-added tables, create a multiplexing operator that handles all their records
        //     and writes to multiple tables
        DataStream<CdcMultiplexRecord> newlyAddedTableStream =
                SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed, CdcDynamicTableParsingProcessFunction.DYNAMIC_OUTPUT_TAG);
        // handles schema change for newly added tables
        SingleOutputStreamOperatorUtils.getSideOutput(
                        parsed,
                        CdcDynamicTableParsingProcessFunction.DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG)
                .keyBy(t -> t.f0)
                .process(new MultiTableUpdatedDataFieldsProcessFunction(catalogLoader))
                .name("Schema Evolution");

        DataStream<CdcMultiplexRecord> partitioned =
                partition(
                        newlyAddedTableStream,
                        new CdcMultiplexRecordChannelComputer(catalogLoader),
                        parallelism);

        FlinkCdcMultiTableSink sink =
                new FlinkCdcMultiTableSink(
                        catalogLoader, committerCpu, committerMemory, commitChaining);
        sink.sinkFrom(partitioned);
    }

    private void buildForFixedBucket(FileStoreTable table, DataStream<CdcRecord> parsed) {
        DataStream<CdcRecord> partitioned =
                partition(parsed, new CdcRecordChannelComputer(table.schema()), parallelism);
        new CdcFixedBucketSink(table).sinkFrom(partitioned);
    }

    private void buildForUnawareBucket(FileStoreTable table, DataStream<CdcRecord> parsed) {
        new CdcUnawareBucketSink(table, parallelism).sinkFrom(parsed);
    }

    private void buildDividedCdcSink() {
        Preconditions.checkNotNull(tables);

        SingleOutputStreamOperator<Void> parsed =
                input.forward()
                        .process(new CdcMultiTableParsingProcessFunction<>(parserFactory))
                        .setParallelism(input.getParallelism());

        for (FileStoreTable table : tables) {
            DataStream<Void> schemaChangeProcessFunction =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                                    parsed,
                                    CdcMultiTableParsingProcessFunction
                                            .createUpdatedDataFieldsOutputTag(table.name()))
                            .process(
                                    new UpdatedDataFieldsProcessFunction(
                                            new SchemaManager(table.fileIO(), table.location()),
                                            Identifier.create(database, table.name()),
                                            catalogLoader));
            schemaChangeProcessFunction.getTransformation().setParallelism(1);
            schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);

            DataStream<CdcRecord> parsedForTable =
                    SingleOutputStreamOperatorUtils.getSideOutput(
                            parsed,
                            CdcMultiTableParsingProcessFunction.createRecordOutputTag(
                                    table.name()));

            BucketMode bucketMode = table.bucketMode();
            switch (bucketMode) {
                case FIXED:
                    buildForFixedBucket(table, parsedForTable);
                    break;
                case DYNAMIC:
                    new CdcDynamicBucketSink(table).build(parsedForTable, parallelism);
                    break;
                case UNAWARE:
                    buildForUnawareBucket(table, parsedForTable);
                    break;
                case GLOBAL_DYNAMIC:
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported bucket mode: " + bucketMode);
            }
        }
    }
}
