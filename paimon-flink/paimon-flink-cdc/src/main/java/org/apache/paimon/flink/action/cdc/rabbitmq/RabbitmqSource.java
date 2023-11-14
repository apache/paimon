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

import org.apache.paimon.utils.StringUtils;

import com.rabbitmq.client.BuiltinExchangeType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;

public class RabbitmqSource extends RMQSource<String> {
    private String exchange;
    private BuiltinExchangeType exchangeType;
    private String routingKey;

    public RabbitmqSource(
            RMQConnectionConfig rmqConnectionConfig,
            String exchange,
            BuiltinExchangeType exchangeType,
            String routingKey,
            String queueName,
            DeserializationSchema<String> deserializationSchema) {
        super(rmqConnectionConfig, queueName, deserializationSchema);
        this.exchange = exchange;
        this.exchangeType = exchangeType;
        this.routingKey = routingKey;
    }

    @Override
    protected void setupQueue() throws IOException {
        super.setupQueue();
        if (!StringUtils.isBlank(exchange) && exchangeType != null) {
            channel.exchangeDeclare(exchange, exchangeType);
            channel.queueBind(
                    queueName, exchange, StringUtils.isBlank(routingKey) ? "" : routingKey);
        }
    }
}
