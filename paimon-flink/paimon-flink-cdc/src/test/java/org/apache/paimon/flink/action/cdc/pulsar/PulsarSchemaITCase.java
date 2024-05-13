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

package org.apache.paimon.flink.action.cdc.pulsar;

import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
import org.apache.paimon.flink.action.cdc.serialization.CdcJsonDeserializationSchema;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.TOPIC;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.VALUE_FORMAT;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.createPulsarConsumer;
import static org.apache.paimon.flink.action.cdc.pulsar.PulsarActionUtils.getDataFormat;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for building schema from Pulsar. */
public class PulsarSchemaITCase extends PulsarActionITCaseBase {
    private static final String FORMAT = DataFormat.CANAL_JSON.asConfigString();

    @Test
    @Timeout(60)
    public void testPulsarSchema() throws Exception {
        String topic = "test_pulsar_schema";
        createTopic(topic);

        // ---------- Write the Canal json into pulsar -------------------
        List<String> messages =
                getMessages("kafka/%s/table/schemaevolution/%s-data-1.txt", FORMAT, FORMAT);
        sendMessages(topic, messages);

        Configuration pulsarConfig = Configuration.fromMap(getBasicPulsarConfig());
        pulsarConfig.setString(TOPIC.key(), topic);
        pulsarConfig.set(VALUE_FORMAT, FORMAT);

        Schema pulsarSchema =
                MessageQueueSchemaUtils.getSchema(
                        createPulsarConsumer(pulsarConfig, new CdcJsonDeserializationSchema()),
                        getDataFormat(pulsarConfig),
                        TypeMapping.defaultMapping());
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pt", DataTypes.INT()));
        fields.add(new DataField(1, "_id", DataTypes.INT().notNull()));
        fields.add(new DataField(2, "v1", DataTypes.VARCHAR(10)));
        assertThat(pulsarSchema.fields()).isEqualTo(fields);
    }
}
