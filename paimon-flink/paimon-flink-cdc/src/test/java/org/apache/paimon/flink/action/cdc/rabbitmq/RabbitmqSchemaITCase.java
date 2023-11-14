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
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import com.rabbitmq.client.BuiltinExchangeType;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.rabbitmq.RabbitmqActionUtils.EXCHANGE;
import static org.apache.paimon.flink.action.cdc.rabbitmq.RabbitmqActionUtils.EXCHANGE_TYPE;
import static org.apache.paimon.flink.action.cdc.rabbitmq.RabbitmqActionUtils.QUEUE_NAME;
import static org.apache.paimon.flink.action.cdc.rabbitmq.RabbitmqActionUtils.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.rabbitmq.RabbitmqActionUtils.getDataFormat;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for building schema from Rabbitmq. */
public class RabbitmqSchemaITCase extends RabbitmqActionITCaseBase {

    @Test
    @Timeout(120)
    public void testPulsarSchema() throws Exception {
        String queue = "test_rabbitmq_schema";
        createQueue(exchange, queue);

        // ---------- Write the Canal json into rabbitmq -------------------
        List<String> messages = getMessages("kafka/canal/table/schemaevolution/canal-data-1.txt");
        sendMessages(exchange, queue, messages);

        Configuration rabbitmqConfig = Configuration.fromMap(getBasicRabbitmqConfig());
        rabbitmqConfig.set(QUEUE_NAME, queue);
        rabbitmqConfig.set(EXCHANGE, exchange);
        rabbitmqConfig.set(EXCHANGE_TYPE, BuiltinExchangeType.FANOUT.name());
        rabbitmqConfig.set(VALUE_FORMAT, "canal-json");

        Schema rabbitmqSchema =
                MessageQueueSchemaUtils.getSchema(
                        RabbitmqActionUtils.createRabbitmqConsumer(rabbitmqConfig, queue),
                        queue,
                        getDataFormat(rabbitmqConfig),
                        TypeMapping.defaultMapping());
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pt", DataTypes.INT()));
        fields.add(new DataField(1, "_id", DataTypes.INT().notNull()));
        fields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        assertThat(rabbitmqSchema.fields()).isEqualTo(fields);
    }
}
