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

package org.apache.paimon.rest;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.schema.TableSchemaSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import static org.apache.paimon.utils.JsonSerdeUtil.registerJsonObjects;

/** Object mapper for REST request and response. */
public class RESTObjectMapper {
    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(createPaimonRestJacksonModule());
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }

    public static Module createPaimonRestJacksonModule() {
        SimpleModule module = new SimpleModule("Paimon_REST");
        registerJsonObjects(
                module,
                TableSchema.class,
                TableSchemaSerializer.INSTANCE,
                TableSchemaSerializer.INSTANCE);
        registerJsonObjects(
                module,
                DataField.class,
                DataField::serializeJson,
                DataTypeJsonParser::parseDataField);
        registerJsonObjects(
                module, DataType.class, DataType::serializeJson, DataTypeJsonParser::parseDataType);
        return module;
    }
}
