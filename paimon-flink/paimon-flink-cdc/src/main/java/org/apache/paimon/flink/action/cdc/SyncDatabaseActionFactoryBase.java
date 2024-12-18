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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.EXCLUDING_DBS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.EXCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.INCLUDING_DBS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.INCLUDING_TABLES;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MULTIPLE_TABLE_PARTITION_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PARTITION_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.PRIMARY_KEYS;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_MAPPING;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_PREFIX;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_PREFIX_DB;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_SUFFIX;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TABLE_SUFFIX_DB;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.TYPE_MAPPING;

/** Base {@link ActionFactory} for synchronizing into database. */
public abstract class SyncDatabaseActionFactoryBase<T extends SyncDatabaseActionBase>
        extends SynchronizationActionFactoryBase<T> {

    protected String database;

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        this.database = params.getRequired(DATABASE);
        return super.create(params);
    }

    @Override
    protected void withParams(MultipleParameterToolAdapter params, T action) {
        action.withTablePrefix(params.get(TABLE_PREFIX))
                .withTableSuffix(params.get(TABLE_SUFFIX))
                .withDbPrefix(optionalConfigMap(params, TABLE_PREFIX_DB))
                .withDbSuffix(optionalConfigMap(params, TABLE_SUFFIX_DB))
                .withTableMapping(optionalConfigMap(params, TABLE_MAPPING))
                .includingTables(params.get(INCLUDING_TABLES))
                .excludingTables(params.get(EXCLUDING_TABLES))
                .includingDbs(params.get(INCLUDING_DBS))
                .excludingDbs(params.get(EXCLUDING_DBS))
                .withPartitionKeyMultiple(
                        optionalConfigMapList(params, MULTIPLE_TABLE_PARTITION_KEYS))
                .withPartitionKeys();

        if (params.has(PARTITION_KEYS)) {
            action.withPartitionKeys(params.get(PARTITION_KEYS).split(","));
        }

        if (params.has(PRIMARY_KEYS)) {
            action.withPrimaryKeys(params.get(PRIMARY_KEYS).split(","));
        }

        if (params.has(TYPE_MAPPING)) {
            String[] options = params.get(TYPE_MAPPING).split(",");
            action.withTypeMapping(TypeMapping.parse(options));
        }
    }
}
