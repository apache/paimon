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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import javax.annotation.Nullable;

/**
 * DataStream API for building Flink Sink for {@link RichCdcRecord} to write with schema evolution.
 *
 * @since 0.8
 */
@Public
public class RichCdcSinkBuilder {

    private DataStream<RichCdcRecord> input = null;
    private Table table = null;
    private Identifier identifier = null;
    private CatalogLoader catalogLoader = null;

    @Nullable private Integer parallelism;

    public RichCdcSinkBuilder(Table table) {
        this.table = table;
    }

    public RichCdcSinkBuilder forRichCdcRecord(DataStream<RichCdcRecord> input) {
        this.input = input;
        return this;
    }

    public RichCdcSinkBuilder identifier(Identifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public RichCdcSinkBuilder parallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public RichCdcSinkBuilder catalogLoader(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }

    public DataStreamSink<?> build() {
        CdcSinkBuilder<RichCdcRecord> builder = new CdcSinkBuilder<>();
        return builder.withTable(table)
                .withInput(input)
                .withParserFactory(RichEventParser::new)
                .withParallelism(parallelism)
                .withIdentifier(identifier)
                .withCatalogLoader(catalogLoader)
                .build();
    }

    // ====================== Deprecated ============================

    /** @deprecated Use constructor to pass table. */
    @Deprecated
    public RichCdcSinkBuilder() {}

    /** @deprecated Use {@link #forRichCdcRecord}. */
    @Deprecated
    public RichCdcSinkBuilder withInput(DataStream<RichCdcRecord> input) {
        this.input = input;
        return this;
    }

    /** @deprecated Use constructor to pass Table. */
    @Deprecated
    public RichCdcSinkBuilder withTable(Table table) {
        this.table = table;
        return this;
    }

    /** @deprecated Use {@link #parallelism}. */
    @Deprecated
    public RichCdcSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /** @deprecated Use {@link #identifier}. */
    @Deprecated
    public RichCdcSinkBuilder withIdentifier(Identifier identifier) {
        this.identifier = identifier;
        return this;
    }

    /** @deprecated Use {@link #catalogLoader}. */
    @Deprecated
    public RichCdcSinkBuilder withCatalogLoader(CatalogLoader catalogLoader) {
        this.catalogLoader = catalogLoader;
        return this;
    }
}
