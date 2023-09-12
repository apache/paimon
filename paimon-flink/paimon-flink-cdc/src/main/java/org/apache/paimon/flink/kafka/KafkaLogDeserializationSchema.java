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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.ProjectedRowData;
import org.apache.paimon.flink.Projection;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.stream.IntStream;

/** A {@link KafkaDeserializationSchema} for the table with primary key in log store. */
public class KafkaLogDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<RowData> producedType;
    private final int fieldCount;
    private final int[] primaryKey;
    @Nullable private final DeserializationSchema<RowData> primaryKeyDeserializer;
    private final DeserializationSchema<RowData> valueDeserializer;
    private final RowData.FieldGetter[] keyFieldGetters;
    @Nullable private final int[][] projectFields;

    private transient ProjectCollector projectCollector;

    public KafkaLogDeserializationSchema(
            DataType physicalType,
            int[] primaryKey,
            @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            @Nullable int[][] projectFields) {
        this.primaryKey = primaryKey;
        this.primaryKeyDeserializer = primaryKeyDeserializer;
        this.valueDeserializer = valueDeserializer;
        RowType logicalType = (RowType) physicalType.getLogicalType();
        this.producedType =
                InternalTypeInfo.of(
                        projectFields == null
                                ? logicalType
                                : Projection.of(projectFields).project(logicalType));
        this.fieldCount = physicalType.getChildren().size();
        this.projectFields = projectFields;
        this.keyFieldGetters =
                IntStream.range(0, primaryKey.length)
                        .mapToObj(
                                i ->
                                        createNullCheckingFieldGetter(
                                                physicalType
                                                        .getChildren()
                                                        .get(primaryKey[i])
                                                        .getLogicalType(),
                                                i))
                        .toArray(RowData.FieldGetter[]::new);
    }

    private static RowData.FieldGetter createNullCheckingFieldGetter(
            LogicalType dataType, int index) {
        RowData.FieldGetter getter = RowData.createFieldGetter(dataType, index);
        if (dataType.isNullable()) {
            return getter;
        } else {
            return row -> {
                if (row.isNullAt(index)) {
                    return null;
                }
                return getter.getFieldOrNull(row);
            };
        }
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (primaryKeyDeserializer != null) {
            primaryKeyDeserializer.open(context);
        }
        valueDeserializer.open(context);
        projectCollector = new ProjectCollector();
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
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<RowData> underCollector)
            throws Exception {
        Collector<RowData> collector = projectCollector.project(underCollector);

        if (primaryKey.length > 0 && record.value() == null) {
            RowData key = Objects.requireNonNull(primaryKeyDeserializer).deserialize(record.key());
            GenericRowData value = new GenericRowData(RowKind.DELETE, fieldCount);
            for (int i = 0; i < primaryKey.length; i++) {
                value.setField(primaryKey[i], keyFieldGetters[i].getFieldOrNull(key));
            }
            collector.collect(value);
        } else {
            valueDeserializer.deserialize(record.value(), collector);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    private class ProjectCollector implements Collector<RowData> {

        private final ProjectedRowData projectedRow =
                projectFields == null ? null : ProjectedRowData.from(projectFields);

        private Collector<RowData> underCollector;

        private Collector<RowData> project(Collector<RowData> underCollector) {
            if (projectedRow == null) {
                return underCollector;
            }

            this.underCollector = underCollector;
            return this;
        }

        @Override
        public void collect(RowData rowData) {
            underCollector.collect(projectedRow.replaceRow(rowData));
        }

        @Override
        public void close() {}
    }
}
