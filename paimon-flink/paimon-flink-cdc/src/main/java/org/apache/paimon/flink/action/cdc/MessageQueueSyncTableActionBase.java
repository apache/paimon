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
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.flink.action.cdc.watermark.MessageQueueCdcTimestampExtractor;
import org.apache.paimon.schema.Schema;

import java.util.Map;

/**
 * Base {@link Action} for synchronizing one message queue topic into one Paimon table.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified topics. If the Paimon table already exists, its
 * schema will be compared against the schema of all specified topics.
 *
 * <p>This action supports a limited number of schema changes. Unsupported schema changes will be
 * ignored. Currently supported schema changes includes:
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
 *       are supported. Other type changes will cause exceptions.
 * </ul>
 */
public abstract class MessageQueueSyncTableActionBase extends SyncTableActionBase {

    public MessageQueueSyncTableActionBase(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mqConfig,
            SyncJobHandler.SourceType sourceType) {
        super(database, table, catalogConfig, mqConfig, sourceType);
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        try (MessageQueueSchemaUtils.ConsumerWrapper consumer = syncJobHandler.provideConsumer()) {
            return MessageQueueSchemaUtils.getSchema(
                    consumer, syncJobHandler.provideDataFormat(), typeMapping);
        }
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return new MessageQueueCdcTimestampExtractor();
    }

    @Override
    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
                table,
                partitionKeys,
                primaryKeys,
                computedColumns,
                tableConfig,
                retrievedSchema,
                metadataConverters,
                caseSensitive,
                true,
                false);
    }
}
