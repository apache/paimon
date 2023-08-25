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

package org.apache.paimon.flink.sink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.Options;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;

/** this is a doc. */
public class MultiTablesCompactorSinkBuilder {
    private final Catalog.Loader catalogLoader;

    private final Options options;

    private DataStream<RowData> input;

    public MultiTablesCompactorSinkBuilder(Catalog.Loader catalogLoader, Options options) {
        this.catalogLoader = catalogLoader;
        this.options = options;
    }

    public MultiTablesCompactorSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public DataStreamSink<?> build() {
        // Currently, multi-tables compaction do not support tables which bucketmode is UNWARE.
        return buildForBucketAware();
    }

    private DataStreamSink<?> buildForBucketAware() {
        DataStream<RowData> partitioned = partition(input, new BucketsRowChannelComputer(), null);
        return new MultiTablesCompactorSink(catalogLoader, options).sinkFrom(partitioned);
    }
}
