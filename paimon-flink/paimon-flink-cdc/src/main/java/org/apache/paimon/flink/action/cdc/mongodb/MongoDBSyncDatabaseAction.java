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

import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionBase;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;

import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;

import java.util.Collections;
import java.util.Map;

/**
 * An action class responsible for synchronizing MongoDB databases with a target system.
 *
 * <p>This class provides functionality to read data from a MongoDB source, process it, and then
 * synchronize it with a target system. It supports various configurations, including table
 * prefixes, suffixes, and inclusion/exclusion patterns.
 *
 * <p>Key features include:
 *
 * <ul>
 *   <li>Support for case-sensitive and case-insensitive database and table names.
 *   <li>Configurable table name conversion with prefixes and suffixes.
 *   <li>Ability to include or exclude specific tables using regular expressions.
 *   <li>Integration with Flink's streaming environment for data processing.
 * </ul>
 *
 * <p>Note: This action is primarily intended for use in Flink streaming applications that
 * synchronize MongoDB data with other systems.
 */
public class MongoDBSyncDatabaseAction extends SyncDatabaseActionBase {

    public MongoDBSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mongodbConfig) {
        super(warehouse, database, catalogConfig, mongodbConfig, SyncJobHandler.SourceType.MONGODB);
    }

    @Override
    protected MongoDBSource<CdcSourceRecord> buildSource() {
        return MongoDBActionUtils.buildMongodbSource(
                cdcSourceConfig,
                CdcActionCommonUtils.combinedModeTableList(
                        cdcSourceConfig.get(MongoDBSourceOptions.DATABASE),
                        includingTables,
                        Collections.emptyList()));
    }
}
