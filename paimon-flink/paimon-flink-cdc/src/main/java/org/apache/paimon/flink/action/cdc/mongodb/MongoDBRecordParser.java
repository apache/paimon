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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.mongodb.strategy.Mongo4VersionStrategy;
import org.apache.paimon.flink.action.cdc.mongodb.strategy.MongoVersionStrategy;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * A parser for MongoDB Debezium JSON records, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 *
 * <p>This parser is designed to process and transform incoming MongoDB Debezium JSON records into a
 * more structured format suitable for further processing. It takes into account the version of
 * MongoDB and applies the appropriate strategy for extracting records.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Support for case-sensitive and case-insensitive field names.
 *   <li>Integration with a configurable table name converter.
 *   <li>Ability to work with different MongoDB version strategies (e.g., Mongo4, Mongo6).
 * </ul>
 *
 * <p>Note: This parser is primarily intended for use in Flink streaming applications that process
 * MongoDB CDC data.
 */
public class MongoDBRecordParser
        implements FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> {

    private static final String FIELD_DATABASE = "db";
    private static final String FIELD_TABLE = "coll";
    private static final String FIELD_NAMESPACE = "ns";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final List<ComputedColumn> computedColumns;
    private final Configuration mongodbConfig;
    private JsonNode root;

    public MongoDBRecordParser(List<ComputedColumn> computedColumns, Configuration mongodbConfig) {
        this.computedColumns = computedColumns;
        this.mongodbConfig = mongodbConfig;
    }

    @Override
    public void flatMap(CdcSourceRecord value, Collector<RichCdcMultiplexRecord> out)
            throws Exception {
        root = value.getJsonNode();
        String databaseName = extractString(FIELD_DATABASE);
        String collection = extractString(FIELD_TABLE);
        MongoVersionStrategy versionStrategy =
                VersionStrategyFactory.create(
                        databaseName, collection, computedColumns, mongodbConfig);
        versionStrategy.extractRecords(root).forEach(out::collect);
    }

    private String extractString(String key) {
        return root.get(FIELD_NAMESPACE).get(key).asText();
    }

    private static class VersionStrategyFactory {
        static MongoVersionStrategy create(
                String databaseName,
                String collection,
                List<ComputedColumn> computedColumns,
                Configuration mongodbConfig) {
            // TODO: When MongoDB CDC is upgraded to 2.5, uncomment the version check logic
            // if (mongodbVersion >= 6) {
            //     return new Mongo6VersionStrategy(databaseName, collection, caseSensitive);
            // }
            return new Mongo4VersionStrategy(
                    databaseName, collection, computedColumns, mongodbConfig);
        }
    }
}
