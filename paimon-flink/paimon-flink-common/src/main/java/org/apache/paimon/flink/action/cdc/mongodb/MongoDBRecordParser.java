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

import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.mongodb.strategy.Mongo4VersionStrategy;
import org.apache.paimon.flink.action.cdc.mongodb.strategy.MongoVersionStrategy;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** Convert MongoDB Debezium JSON string to list of {@link RichCdcMultiplexRecord}s. */
public class MongoDBRecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    private static final String FIELD_DATABASE = "db";
    private static final String FIELD_TABLE = "coll";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    private final Configuration mongodbConfig;
    private JsonNode root;

    public MongoDBRecordParser(boolean caseSensitive, Configuration mongodbConfig) {

        this(caseSensitive, new TableNameConverter(caseSensitive), mongodbConfig);
    }

    public MongoDBRecordParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            Configuration mongodbConfig) {
        this.caseSensitive = caseSensitive;
        this.tableNameConverter = tableNameConverter;
        this.mongodbConfig = mongodbConfig;
    }

    @Override
    public void flatMap(String value, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = objectMapper.readValue(value, JsonNode.class);
        String databaseName = extractString(FIELD_DATABASE);
        String collection = tableNameConverter.convert(extractString(FIELD_TABLE));
        MongoVersionStrategy versionStrategy =
                new Mongo4VersionStrategy(databaseName, collection, caseSensitive, mongodbConfig);

        // TODO:Upgrade mongodb cdc to 2.5 and enable scanFullChangelog capability
        // [Full Changelog]
        // https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mongodb-cdc.html
        //        if (mongodbVersion >= 6) {
        //            versionStrategy = new Mongo6VersionStrategy(databaseName, collection,
        // caseSensitive);
        //        } else {
        //            versionStrategy = new Mongo4VersionStrategy(databaseName, collection,
        // caseSensitive);
        //        }
        versionStrategy.extractRecords(root).forEach(out::collect);
    }

    private String extractString(String key) {
        return root.get("ns").get(key).asText();
    }
}
