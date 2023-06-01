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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final Set<String> initialTables;
    private final String database;
    private final Catalog.Loader catalogLoader;

    private transient EventParser<T> parser;
    private transient Catalog catalog;
    private transient Map<String, OutputTag<List<DataField>>> updatedDataFieldsOutputTags;
    private transient Map<String, OutputTag<CdcRecord>> recordOutputTags;
    public static final OutputTag<CdcMultiplexRecord> NEW_TABLE_OUTPUT_TAG =
            new OutputTag<>(
                    "paimon-newly-added-table", TypeInformation.of(CdcMultiplexRecord.class));
    public static final OutputTag<Tuple2<Identifier, List<DataField>>>
            NEW_TABLE_SCHEMA_CHANGE_OUTPUT_TAG =
                    new OutputTag<>(
                            "paimon-newly-added-table-schema-change",
                            TypeInformation.of(
                                    new TypeHint<Tuple2<Identifier, List<DataField>>>() {}));

    public CdcMultiTableParsingProcessFunction(
            String database,
            Catalog.Loader catalogLoader,
            List<FileStoreTable> tables,
            EventParser.Factory<T> parserFactory) {
        // for now, only support single database
        this.database = database;
        this.catalogLoader = catalogLoader;
        this.initialTables = tables.stream().map(FileStoreTable::name).collect(Collectors.toSet());
        this.parserFactory = parserFactory;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        updatedDataFieldsOutputTags = new HashMap<>();
        recordOutputTags = new HashMap<>();
        catalog = catalogLoader.load();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);

        // CDC Ingestion only supports single database at this time being.
        //    In the future, there will be a mapping between source databases
        //    and target paimon databases
        String databaseName = parser.parseDatabaseName();
        String tableName = parser.parseTableName();

        // check for newly added table
        parser.parseNewSchema(database)
                .ifPresent(
                        schema -> {
                            Identifier identifier =
                                    new Identifier(database, parser.parseTableName());
                            try {
                                catalog.createTable(identifier, schema, true);
                            } catch (Throwable ignored) {
                            }
                        });

        List<DataField> schemaChange = parser.parseSchemaChange();
        if (schemaChange.size() > 0) {
            if (isTableNewlyAdded(tableName)) {
                context.output(
                        NEW_TABLE_SCHEMA_CHANGE_OUTPUT_TAG,
                        Tuple2.of(Identifier.create(database, tableName), schemaChange));
            } else {
                context.output(getUpdatedDataFieldsOutputTag(tableName), schemaChange);
            }
        }

        if (isTableNewlyAdded(tableName)) {
            parser.parseRecords()
                    .forEach(
                            record ->
                                    context.output(
                                            NEW_TABLE_OUTPUT_TAG,
                                            wrapRecord(database, tableName, record)));

        } else {
            parser.parseRecords()
                    .forEach(record -> context.output(getRecordOutputTag(tableName), record));
        }
    }

    private CdcMultiplexRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return CdcMultiplexRecord.fromCdcRecord(databaseName, tableName, record);
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

    private boolean isTableNewlyAdded(String tableName) {
        return !initialTables.contains(tableName);
    }

    public static OutputTag<CdcRecord> createRecordOutputTag(String tableName) {
        return new OutputTag<>("record-" + tableName, TypeInformation.of(CdcRecord.class));
    }
}
