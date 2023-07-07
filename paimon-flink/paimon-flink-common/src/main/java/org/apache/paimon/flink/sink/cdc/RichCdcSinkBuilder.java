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
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import javax.annotation.Nullable;

/** Builder for sink when syncing {@link RichCdcRecord} records into one Paimon table. */
@Experimental
public class RichCdcSinkBuilder {

    private DataStream<RichCdcRecord> input = null;
    private Table table = null;
    private Identifier identifier = null;
    private Catalog.Loader catalogLoader = null;

    @Nullable private Integer parallelism;

    public RichCdcSinkBuilder withInput(DataStream<RichCdcRecord> input) {
        this.input = input;
        return this;
    }

    public RichCdcSinkBuilder withTable(Table table) {
        this.table = table;
        return this;
    }

    public RichCdcSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public RichCdcSinkBuilder withIdentifier(Identifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public RichCdcSinkBuilder withCatalogLoader(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public DataStreamSink<?> build() {
        CdcSinkBuilder<RichCdcRecord> builder = new CdcSinkBuilder<>();
        return builder.withTable(table)
                .withInput(input)
                .withParserFactory(new RichCdcParserFactory())
                .withParallelism(parallelism)
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();
    }
}
