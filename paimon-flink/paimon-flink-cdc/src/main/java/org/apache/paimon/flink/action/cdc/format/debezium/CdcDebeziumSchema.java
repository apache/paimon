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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Cdc Debezium Schema Entity. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CdcDebeziumSchema {

    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FIELD = "field";
    private static final String FIELD_OPTIONAL = "optional";

    @JsonProperty(FIELD_FIELD)
    private final String field;

    @JsonProperty(FIELD_TYPE)
    private final String type;

    @JsonProperty(FIELD_OPTIONAL)
    private final Boolean optional;

    @JsonProperty(FIELD_FIELDS)
    private final List<CdcDebeziumSchema> fields;

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonCreator
    public CdcDebeziumSchema(
            @JsonProperty(FIELD_FIELD) String field,
            @JsonProperty(FIELD_TYPE) String type,
            @JsonProperty(FIELD_OPTIONAL) Boolean optional,
            @JsonProperty(FIELD_FIELDS) List<CdcDebeziumSchema> fields,
            @JsonProperty(FIELD_NAME) String name) {
        this.field = field;
        this.type = type;
        this.optional = optional;
        this.fields = fields;
        this.name = name;
    }

    @JsonGetter(FIELD_FIELD)
    public String field() {
        return field;
    }

    @JsonGetter(FIELD_TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELD_OPTIONAL)
    public Boolean optional() {
        return optional;
    }

    @JsonGetter(FIELD_FIELDS)
    public List<CdcDebeziumSchema> fields() {
        return fields;
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }
}
