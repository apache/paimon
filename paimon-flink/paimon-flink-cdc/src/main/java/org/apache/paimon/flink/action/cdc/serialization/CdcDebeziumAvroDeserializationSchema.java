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

package org.apache.paimon.flink.action.cdc.serialization;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.format.avro.AvroSchemaConverter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.debezium.DebeziumAvroDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * A simple deserialization schema for {@link CdcSourceRecord}. reference from {@link
 * DebeziumAvroDeserializationSchema}.
 */
public class CdcDebeziumAvroDeserializationSchema
        implements DeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CdcJsonDeserializationSchema.class);

    /** The deserializer to deserialize Debezium Avro data. */
    private final DeserializationSchema<GenericRecord> avroDeserializer;

    public CdcDebeziumAvroDeserializationSchema(
            RowType rowType,
            String schemaRegistryUrl,
            @Nullable String schemaString,
            @Nullable Map<String, ?> registryConfigs) {
        Schema schema =
                schemaString == null
                        ? AvroSchemaConverter.convertToSchema(rowType)
                        : new Parser().parse(schemaString);
        avroDeserializer =
                ConfluentRegistryAvroDeserializationSchema.forGeneric(
                        schema, schemaRegistryUrl, registryConfigs);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        avroDeserializer.open(context);
    }

    @Override
    public CdcSourceRecord deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return null;
        }

        GenericRecord record = this.avroDeserializer.deserialize(message);
        return new CdcSourceRecord(record);
    }

    @Override
    public boolean isEndOfStream(CdcSourceRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }

    public static RowType createDebeziumAvroRowType(DataType databaseSchema) {
        // Debezium Avro contains other information, e.g. "source", "ts_ms"
        // but we don't need them
        return DataTypes.ROW(
                DataTypes.FIELD(0, "before", databaseSchema.nullable()),
                DataTypes.FIELD(1, "after", databaseSchema.nullable()),
                DataTypes.FIELD(2, "op", DataTypes.STRING()));
    }
}
