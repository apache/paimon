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

import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Source;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Represents an action to synchronize a specific MongoDB table with a target system.
 *
 * <p>This action is responsible for:
 *
 * <ul>
 *   <li>Validating the provided MongoDB configuration.
 *   <li>Checking and ensuring the existence of the target database and table.
 *   <li>Setting up the necessary Flink streaming environment for data synchronization.
 *   <li>Handling case sensitivity considerations for database and table names.
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>
 * MongoDBSyncTableAction action = new MongoDBSyncTableAction(...);
 * action.run();
 * </pre>
 */
public class MongoDBSyncTableAction extends SyncTableActionBase {

    public MongoDBSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mongodbConfig) {
        super(warehouse, database, table, catalogConfig, mongodbConfig);
    }

    @Override
    protected void checkCdcSourceArgument() {
        checkArgument(
                cdcSourceConfig.contains(MongoDBSourceOptions.COLLECTION),
                String.format(
                        "mongodb-conf [%s] must be specified.",
                        MongoDBSourceOptions.COLLECTION.key()));
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        boolean caseSensitive = catalog.caseSensitive();
        return MongodbSchemaUtils.getMongodbSchema(cdcSourceConfig, caseSensitive);
    }

    @Override
    protected Source<String, ?, ?> buildSource() throws Exception {
        String tableList =
                cdcSourceConfig.get(MongoDBSourceOptions.DATABASE)
                        + "\\."
                        + cdcSourceConfig.get(MongoDBSourceOptions.COLLECTION);
        return MongoDBActionUtils.buildMongodbSource(cdcSourceConfig, tableList);
    }

    @Override
    protected String sourceName() {
        return "MongoDB Source";
    }

    @Override
    protected FlatMapFunction<String, RichCdcMultiplexRecord> recordParse() {
        boolean caseSensitive = catalog.caseSensitive();
        return new MongoDBRecordParser(caseSensitive, computedColumns, cdcSourceConfig);
    }

    @Override
    protected String jobName() {
        return String.format("MongoDB-Paimon Table Sync: %s.%s", database, table);
    }
}
