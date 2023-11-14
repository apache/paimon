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

package org.apache.paimon.flink.action.cdc.rabbitmq;

import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.MessageQueueSyncTableActionBase;
import org.apache.paimon.flink.action.cdc.format.DataFormat;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;

/** Synchronize table from Rabbitmq. */
public class RabbitmqSyncTableAction extends MessageQueueSyncTableActionBase {

    public RabbitmqSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mqConfig) {
        super(warehouse, database, table, catalogConfig, mqConfig);
    }

    @Override
    protected SourceFunction<String> buildSource() {
        return RabbitmqActionUtils.buildRabbitmqSource(mqConfig);
    }

    @Override
    protected String topic() {
        return mqConfig.get(RabbitmqActionUtils.QUEUE_NAME);
    }

    @Override
    protected MessageQueueSchemaUtils.ConsumerWrapper consumer(String topic) {
        return RabbitmqActionUtils.createRabbitmqConsumer(mqConfig, topic);
    }

    @Override
    protected DataFormat getDataFormat() {
        return RabbitmqActionUtils.getDataFormat(mqConfig);
    }

    @Override
    protected String sourceName() {
        return "Rabbitmq Source";
    }

    @Override
    protected String jobName() {
        return String.format("Rabbitmq-Paimon Table Sync: %s.%s", database, table);
    }
}
