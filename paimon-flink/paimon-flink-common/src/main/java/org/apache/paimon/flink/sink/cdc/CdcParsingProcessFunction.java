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

import org.apache.paimon.schema.SchemaChange;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A {@link ProcessFunction} to parse CDC change event to either {@link SchemaChange} or {@link
 * CdcRecord} and send them to different downstreams.
 *
 * @param <T> CDC change event type
 */
public class CdcParsingProcessFunction<T> extends ProcessFunction<T, CdcRecord> {

    public static final OutputTag<SchemaChange> SCHEMA_CHANGE_OUTPUT_TAG =
            new OutputTag<>("schema-change", TypeInformation.of(SchemaChange.class));

    private final EventParser.Factory<T> parserFactory;

    private transient EventParser<T> parser;

    public CdcParsingProcessFunction(EventParser.Factory<T> parserFactory) {
        this.parserFactory = parserFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
    }

    @Override
    public void processElement(T raw, Context context, Collector<CdcRecord> collector)
            throws Exception {
        parser.setRawEvent(raw);
        if (parser.isSchemaChange()) {
            for (SchemaChange schemaChange : parser.getSchemaChanges()) {
                context.output(SCHEMA_CHANGE_OUTPUT_TAG, schemaChange);
            }
        } else {
            for (CdcRecord record : parser.getRecords()) {
                collector.collect(record);
            }
        }
    }
}
