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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.SingleOutputStreamOperatorUtils;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import javax.annotation.Nullable;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/**
 * Builder for sink when syncing records into one Paimon table.
 *
 * @param <T> CDC change event type
 */
@Experimental
public class CdcSinkBuilder<T> {

    private DataStream<T> input = null;
    private EventParser.Factory<T> parserFactory = null;
    private Table table = null;
    private Identifier identifier = null;
    private Catalog.Loader catalogLoader = null;

    @Nullable private Integer parallelism;

    public CdcSinkBuilder<T> withInput(DataStream<T> input) {
        this.input = input;
        return this;
    }

    public CdcSinkBuilder<T> withParserFactory(EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
        return this;
    }

    public CdcSinkBuilder<T> withTable(Table table) {
        this.table = table;
        return this;
    }

    public CdcSinkBuilder<T> withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public CdcSinkBuilder<T> withIdentifier(Identifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public CdcSinkBuilder<T> withCatalogLoader(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public DataStreamSink<?> build() {
        Preconditions.checkNotNull(input, "Input DataStream can not be null.");
        Preconditions.checkNotNull(parserFactory, "Event ParserFactory can not be null.");
        Preconditions.checkNotNull(table, "Paimon Table can not be null.");
        Preconditions.checkNotNull(identifier, "Paimon Table Identifier can not be null.");
        Preconditions.checkNotNull(catalogLoader, "Paimon Catalog Loader can not be null.");

        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException(
                    "Table should be a data table, but is: " + table.getClass().getName());
        }

        FileStoreTable dataTable = (FileStoreTable) table;

        SingleOutputStreamOperator<CdcRecord> parsed =
                input.forward()
                        .process(new CdcParsingProcessFunction<>(parserFactory))
                        .name("Side Output")
                        .setParallelism(input.getParallelism());

        DataStream<Void> schemaChangeProcessFunction =
                SingleOutputStreamOperatorUtils.getSideOutput(
                                parsed, CdcParsingProcessFunction.NEW_DATA_FIELD_LIST_OUTPUT_TAG)
                        .process(
                                new UpdatedDataFieldsProcessFunction(
                                        new SchemaManager(dataTable.fileIO(), dataTable.location()),
                                        identifier,
                                        catalogLoader))
                        .name("Schema Evolution");
        schemaChangeProcessFunction.getTransformation().setParallelism(1, true);
        schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);

        DataStream<CdcRecord> converted =
                CaseSensitiveUtils.cdcRecordConvert(catalogLoader, parsed);
        BucketMode bucketMode = dataTable.bucketMode();
        switch (bucketMode) {
            case HASH_FIXED:
                return buildForFixedBucket(converted);
            case HASH_DYNAMIC:
                return new CdcDynamicBucketSink((FileStoreTable) table)
                        .build(converted, parallelism);
            case BUCKET_UNAWARE:
                return buildForUnawareBucket(converted);
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildForFixedBucket(DataStream<CdcRecord> parsed) {
        FileStoreTable dataTable = (FileStoreTable) table;
        DataStream<CdcRecord> partitioned =
                partition(parsed, new CdcRecordChannelComputer(dataTable.schema()), parallelism);
        return new CdcFixedBucketSink(dataTable).sinkFrom(partitioned);
    }

    private DataStreamSink<?> buildForUnawareBucket(DataStream<CdcRecord> parsed) {
        FileStoreTable dataTable = (FileStoreTable) table;
        return new CdcUnawareBucketSink(dataTable, parallelism).sinkFrom(parsed);
    }
}
