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

import org.apache.paimon.types.DataField;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different side outputs according to table name.
 *
 * <p>This {@link ProcessFunction} can handle records for different tables at the same time.
 *
 * @param <T> CDC change event type
 */
public class CdcMultiTableParsingProcessFunction<T> extends ProcessFunction<T, Void> {

    private final EventParser.Factory<T> parserFactory;

    private transient EventParser<T> parser;
    private transient Map<String, OutputTag<List<DataField>>> updatedDataFieldsOutputTags;
    private transient Map<String, OutputTag<CdcRecord>> recordOutputTags;
    private final Map<String, List<DataField>> dataFiledMap;

    public CdcMultiTableParsingProcessFunction(
            EventParser.Factory<T> parserFactory, Map<String, List<DataField>> dataFiledMap) {
        this.parserFactory = parserFactory;
        this.dataFiledMap = dataFiledMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        updatedDataFieldsOutputTags = new HashMap<>();
        recordOutputTags = new HashMap<>();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);
        String tableName = parser.parseTableName();
        parser.evalComputedColumns(dataFiledMap.get(tableName));

        List<DataField> schemaChange = parser.parseSchemaChange();
        if (!schemaChange.isEmpty()) {
            context.output(getUpdatedDataFieldsOutputTag(tableName), schemaChange);
        }
        parser.parseRecords()
                .forEach(record -> context.output(getRecordOutputTag(tableName), record));
    }

    private OutputTag<List<DataField>> getUpdatedDataFieldsOutputTag(String tableName) {
        return updatedDataFieldsOutputTags.computeIfAbsent(
                tableName, CdcMultiTableParsingProcessFunction::createUpdatedDataFieldsOutputTag);
    }

    public static OutputTag<List<DataField>> createUpdatedDataFieldsOutputTag(String tableName) {
        return new OutputTag<>(
                "new-data-field-list-" + tableName, new ListTypeInfo<>(DataField.class));
    }

    private OutputTag<CdcRecord> getRecordOutputTag(String tableName) {
        return recordOutputTags.computeIfAbsent(
                tableName, CdcMultiTableParsingProcessFunction::createRecordOutputTag);
    }

    public static OutputTag<CdcRecord> createRecordOutputTag(String tableName) {
        return new OutputTag<>("record-" + tableName, TypeInformation.of(CdcRecord.class));
    }
}
