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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * A JSON format implementation of {@link DebeziumDeserializationSchema} which deserializes the
 * received {@link SourceRecord} to {@link CdcSourceRecord}.
 */
public class CdcDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    /**
     * Configuration whether to enable {@link JsonConverterConfig#SCHEMAS_ENABLE_CONFIG} to include
     * schema in messages.
     */
    private final Boolean includeSchema;

    /** The custom configurations for {@link JsonConverter}. */
    private final Map<String, Object> customConverterConfigs;

    public CdcDebeziumDeserializationSchema() {
        this(false);
    }

    public CdcDebeziumDeserializationSchema(Boolean includeSchema) {
        this(includeSchema, null);
    }

    public CdcDebeziumDeserializationSchema(
            Boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs = customConverterConfigs;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<CdcSourceRecord> out) throws Exception {
        if (jsonConverter == null) {
            initializeJsonConverter();
        }
        byte[] bytes =
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        out.collect(new CdcSourceRecord(record.topic(), null, new String(bytes)));
    }

    /** Initialize {@link JsonConverter} with given configs. */
    private void initializeJsonConverter() {
        jsonConverter = new JsonConverter();
        final HashMap<String, Object> configs = new HashMap<>(2);
        configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, includeSchema);
        if (customConverterConfigs != null) {
            configs.putAll(customConverterConfigs);
        }
        jsonConverter.configure(configs);
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
