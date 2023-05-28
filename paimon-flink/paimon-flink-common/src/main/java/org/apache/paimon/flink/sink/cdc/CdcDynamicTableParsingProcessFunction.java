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
import org.apache.paimon.types.DataField;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different side outputs according to table name. This process
 * function will capture newly added tables when syncing entire database and in cases where the
 * newly added tables are including by attesting table filters.
 *
 * <p>This {@link ProcessFunction} can handle records for different tables at the same time.
 *
 * @param <T> CDC change event type
 */
public class CdcDynamicTableParsingProcessFunction<T> extends ProcessFunction<T, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CdcDynamicTableParsingProcessFunction.class);

    public static final OutputTag<CdcMultiplexRecord> DYNAMIC_OUTPUT_TAG =
            new OutputTag<>("paimon-dynamic-table", TypeInformation.of(CdcMultiplexRecord.class));

    public static final OutputTag<Tuple2<Identifier, List<DataField>>>
            DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG =
                    new OutputTag<>(
                            "paimon-dynamic-table-schema-change",
                            TypeInformation.of(
                                    new TypeHint<Tuple2<Identifier, List<DataField>>>() {}));

    private final EventParser.Factory<T> parserFactory;
    private final String database;
    private final Catalog.Loader catalogLoader;
    private final boolean syncToMultipleDB;

    private transient EventParser<T> parser;
    private transient Catalog catalog;

    public CdcDynamicTableParsingProcessFunction(
            String database,
            Catalog.Loader catalogLoader,
            EventParser.Factory<T> parserFactory,
            boolean syncToMultipleDB) {
        this.database = database;
        this.catalogLoader = catalogLoader;
        this.parserFactory = parserFactory;
        this.syncToMultipleDB = syncToMultipleDB;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
        catalog = catalogLoader.load();
    }

    @Override
    public void processElement(T raw, Context context, Collector<Void> collector) throws Exception {
        parser.setRawEvent(raw);

        String finalDatabaseName;
        String finalTableName;
        if (syncToMultipleDB) {
            finalDatabaseName = parser.parseDatabaseName();
            finalTableName = parser.parseTableName().split("\\.")[1];
        } else {
            finalDatabaseName = database;
            finalTableName = parser.parseTableName();
        }

        // check for newly added table
        parser.parseNewTable()
                .ifPresent(
                        schema -> {
                            Identifier identifier =
                                    new Identifier(finalDatabaseName, finalTableName);
                            try {
                                if (!catalog.databaseExists(finalDatabaseName)) {
                                    catalog.createDatabase(finalDatabaseName, true);
                                }
                                catalog.createTable(identifier, schema, true);
                            } catch (Exception e) {
                                LOG.error("create newly added paimon table error.", e);
                            }
                        });

        List<DataField> schemaChange = parser.parseSchemaChange();
        if (schemaChange.size() > 0) {
            context.output(
                    DYNAMIC_SCHEMA_CHANGE_OUTPUT_TAG,
                    Tuple2.of(Identifier.create(finalDatabaseName, finalTableName), schemaChange));
        }

        parser.parseRecords()
                .forEach(
                        record ->
                                context.output(
                                        DYNAMIC_OUTPUT_TAG,
                                        wrapRecord(finalDatabaseName, finalTableName, record)));
    }

    private CdcMultiplexRecord wrapRecord(String databaseName, String tableName, CdcRecord record) {
        return CdcMultiplexRecord.fromCdcRecord(databaseName, tableName, record);
    }
}
