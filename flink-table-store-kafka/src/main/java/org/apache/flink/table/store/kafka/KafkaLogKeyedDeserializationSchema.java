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

package org.apache.flink.table.store.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.stream.IntStream;

/** A {@link KafkaDeserializationSchema} for the table with primary key in log store. */
public class KafkaLogKeyedDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> producedType;
    private final int fieldCount;
    private final int[] primaryKey;
    private final DeserializationSchema<RowData> keyDeserializer;
    private final DeserializationSchema<RowData> valueDeserializer;
    private final RowData.FieldGetter[] keyFieldGetters;

    public KafkaLogKeyedDeserializationSchema(
            DataType physicalType,
            int[] primaryKey,
            DeserializationSchema<RowData> keyDeserializer,
            DeserializationSchema<RowData> valueDeserializer) {
        this.primaryKey = primaryKey;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.producedType = InternalTypeInfo.of(physicalType.getLogicalType());
        this.fieldCount = physicalType.getChildren().size();
        this.keyFieldGetters =
                IntStream.range(0, primaryKey.length)
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                physicalType
                                                        .getChildren()
                                                        .get(primaryKey[i])
                                                        .getLogicalType(),
                                                i))
                        .toArray(RowData.FieldGetter[]::new);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        keyDeserializer.open(context);
        valueDeserializer.open(context);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> record) {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> out)
            throws Exception {
        if (record.value() == null) {
            RowData key = keyDeserializer.deserialize(record.key());
            GenericRowData value = new GenericRowData(RowKind.DELETE, fieldCount);
            for (int i = 0; i < primaryKey.length; i++) {
                value.setField(primaryKey[i], keyFieldGetters[i].getFieldOrNull(key));
            }
            out.collect(value);
        } else {
            valueDeserializer.deserialize(record.value(), out);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
