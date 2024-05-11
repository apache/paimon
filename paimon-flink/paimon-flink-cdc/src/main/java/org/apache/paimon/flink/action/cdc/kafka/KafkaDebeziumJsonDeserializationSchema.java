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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** A simple deserialization schema for {@link CdcSourceRecord}. */
public class KafkaDebeziumJsonDeserializationSchema
        implements KafkaDeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaDebeziumJsonDeserializationSchema.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaDebeziumJsonDeserializationSchema(Configuration cdcSourceConfig) {
        this();
    }

    public KafkaDebeziumJsonDeserializationSchema() {
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public CdcSourceRecord deserialize(ConsumerRecord<byte[], byte[]> message) throws IOException {
        if (message.value() == null) {
            // skip tombstone messages
            return null;
        }

        try {
            return new CdcSourceRecord(objectMapper.readValue(message.value(), JsonNode.class));
        } catch (Exception e) {
            LOG.error("Invalid Json:\n{}", new String(message.value()));
            throw e;
        }
    }

    @Override
    public boolean isEndOfStream(CdcSourceRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
