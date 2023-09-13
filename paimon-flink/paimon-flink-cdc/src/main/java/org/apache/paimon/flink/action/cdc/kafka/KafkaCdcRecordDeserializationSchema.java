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

import org.apache.paimon.flink.action.cdc.kafka.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * Implementation of {@link KafkaRecordDeserializationSchema} for the deserialization of Kafka
 * records which support the Canal, Ogg, Maxwell and Debezium format.
 */
public class KafkaCdcRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<List<RichCdcMultiplexRecord>> {

    private final RecordParser recordParser;

    public KafkaCdcRecordDeserializationSchema(RecordParser recordParser) {
        this.recordParser = recordParser;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        recordParser.open();
    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<List<RichCdcMultiplexRecord>> out)
            throws IOException {
        List<RichCdcMultiplexRecord> records =
                recordParser.extractRecords(record.topic(), record.key(), record.value());
        if (!records.isEmpty()) {
            out.collect(records);
        }
    }

    @Override
    public TypeInformation<List<RichCdcMultiplexRecord>> getProducedType() {
        return new ListTypeInfo<>(getForClass(RichCdcMultiplexRecord.class));
    }
}
