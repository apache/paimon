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
import org.apache.paimon.flink.action.MultiTablesSinkMode;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import org.apache.flink.api.common.functions.FlatMapFunction;

import java.util.Collections;
import java.util.Map;

/**
 * An {@link Action} which synchronize the Multiple message queue topics into one Paimon database.
 *
 * <p>For each message queue topic's table to be synchronized, if the corresponding Paimon table
 * does not exist, this action will automatically create the table, and its schema will be derived
 * from all specified message queue topic's tables. If the Paimon table already exists and its
 * schema is different from that parsed from message queue record, this action will try to preform
 * schema evolution.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 *
 * <p>To automatically synchronize new table, This action creates a single sink for all Paimon
 * tables to be written. See {@link MultiTablesSinkMode#COMBINED}.
 */
public abstract class MessageQueueSyncDatabaseActionBase extends SyncDatabaseActionBase {

    public MessageQueueSyncDatabaseActionBase(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mqConfig) {
        super(warehouse, database, catalogConfig, mqConfig);
    }

    @Override
    protected FlatMapFunction<String, RichCdcMultiplexRecord> recordParse() {
        DataFormat format = getDataFormat();
        boolean caseSensitive = catalog.caseSensitive();
        return format.createParser(caseSensitive, typeMapping, Collections.emptyList());
    }

    protected abstract DataFormat getDataFormat();
}
