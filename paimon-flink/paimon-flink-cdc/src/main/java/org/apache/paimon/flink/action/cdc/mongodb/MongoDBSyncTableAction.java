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
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.schema.Schema;

import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;

import java.util.Map;

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
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mongodbConfig) {
        super(database, table, catalogConfig, mongodbConfig, SyncJobHandler.SourceType.MONGODB);
    }

    @Override
    protected Schema retrieveSchema() {
        return MongodbSchemaUtils.getMongodbSchema(cdcSourceConfig);
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return MongoDBActionUtils.createCdcTimestampExtractor();
    }

    @Override
    protected MongoDBSource<CdcSourceRecord> buildSource() {
        validateRuntimeExecutionMode();
        String tableList =
                cdcSourceConfig.get(MongoDBSourceOptions.DATABASE)
                        + "\\."
                        + cdcSourceConfig.get(MongoDBSourceOptions.COLLECTION);
        return MongoDBActionUtils.buildMongodbSource(cdcSourceConfig, tableList);
    }
}
