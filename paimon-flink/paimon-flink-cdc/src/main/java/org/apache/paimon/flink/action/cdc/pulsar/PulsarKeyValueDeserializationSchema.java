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

import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.Message;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** Deserialization for Pulsar key and value. */
public class PulsarKeyValueDeserializationSchema implements PulsarDeserializationSchema<String> {

    private String charset;
    private String format;

    public PulsarKeyValueDeserializationSchema(String format) {
        this(StandardCharsets.UTF_8.name(), format);
    }

    public PulsarKeyValueDeserializationSchema(String charset, String format) {
        this.charset = Preconditions.checkNotNull(charset);
        this.format = format;
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<String> collector) throws Exception {
        String value = new String(message.getValue(), Charset.forName(charset));
        collector.collect(
                DebeziumSchemaUtils.rewriteValue(Pair.of(message.getKey(), value), format));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
