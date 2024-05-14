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

package org.apache.paimon.flink.action.cdc.mysql.format;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Debezium Event Records Entity. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumEvent {

    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_SCHEMA = "schema";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_HISTORY_RECORD = "historyRecord";
    private static final String FIELD_OP = "op";
    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FIELD = "field";
    private static final String FIELD_OPTIONAL = "optional";
    private static final String FIELD_PARAMETERS = "parameters";

    @JsonProperty(FIELD_PAYLOAD)
    private final Payload payload;

    @JsonProperty(FIELD_SCHEMA)
    private final Field schema;

    @JsonCreator
    public DebeziumEvent(
            @JsonProperty(FIELD_PAYLOAD) Payload payload,
            @JsonProperty(FIELD_SCHEMA) Field schema) {
        this.payload = payload;
        this.schema = schema;
    }

    @JsonGetter(FIELD_PAYLOAD)
    public Payload payload() {
        return payload;
    }

    @JsonGetter(FIELD_SCHEMA)
    public Field schema() {
        return schema;
    }

    /** Payload elements in Debezium event record. */
    public static class Payload {
        @JsonProperty(FIELD_SOURCE)
        private final JsonNode source;

        @JsonProperty(FIELD_BEFORE)
        private final JsonNode before;

        @JsonProperty(FIELD_AFTER)
        private final JsonNode after;

        @JsonProperty(FIELD_HISTORY_RECORD)
        private final String historyRecord;

        @JsonProperty(FIELD_OP)
        private final String op;

        @JsonCreator
        public Payload(
                @JsonProperty(FIELD_SOURCE) JsonNode source,
                @JsonProperty(FIELD_BEFORE) JsonNode before,
                @JsonProperty(FIELD_AFTER) JsonNode after,
                @JsonProperty(FIELD_HISTORY_RECORD) String historyRecord,
                @JsonProperty(FIELD_OP) String op) {
            this.source = source;
            this.before = before;
            this.after = after;
            this.historyRecord = historyRecord;
            this.op = op;
        }

        @JsonGetter(FIELD_SOURCE)
        public JsonNode source() {
            return source;
        }

        @JsonGetter(FIELD_BEFORE)
        public JsonNode before() {
            return before;
        }

        @JsonGetter(FIELD_AFTER)
        public JsonNode after() {
            return after;
        }

        @JsonGetter(FIELD_HISTORY_RECORD)
        public String historyRecord() {
            return historyRecord;
        }

        @JsonGetter(FIELD_OP)
        public String op() {
            return op;
        }

        public boolean isSchemaChange() {
            return op() == null;
        }

        public boolean hasHistoryRecord() {
            return historyRecord != null;
        }

        /** Get table changes in history record. */
        @JsonIgnore
        public Iterator<TableChanges.TableChange> getTableChanges() throws IOException {
            return DebeziumEventUtils.getTableChanges(historyRecord).iterator();
        }
    }

    /** Payload elements in Debezium event record. */
    public static class Field {

        @JsonProperty(FIELD_FIELD)
        private final String field;

        @JsonProperty(FIELD_TYPE)
        private final String type;

        @JsonProperty(FIELD_NAME)
        private final String name;

        @JsonProperty(FIELD_OPTIONAL)
        private final Boolean optional;

        @JsonProperty(FIELD_FIELDS)
        private final List<Field> fields;

        @JsonProperty(FIELD_PARAMETERS)
        private final JsonNode parameters;

        @JsonCreator
        public Field(
                @JsonProperty(FIELD_FIELD) String field,
                @JsonProperty(FIELD_TYPE) String type,
                @JsonProperty(FIELD_NAME) String name,
                @JsonProperty(FIELD_OPTIONAL) Boolean optional,
                @JsonProperty(FIELD_FIELDS) List<Field> fields,
                @JsonProperty(FIELD_PARAMETERS) JsonNode parameters) {
            this.field = field;
            this.type = type;
            this.name = name;
            this.optional = optional;
            this.fields = fields;
            this.parameters = parameters;
        }

        @JsonGetter(FIELD_FIELD)
        public String field() {
            return field;
        }

        @JsonGetter(FIELD_TYPE)
        public String type() {
            return type;
        }

        @JsonGetter(FIELD_NAME)
        public String name() {
            return name;
        }

        @JsonGetter(FIELD_OPTIONAL)
        public Boolean optional() {
            return optional;
        }

        @JsonGetter(FIELD_FIELDS)
        public List<Field> fields() {
            return fields;
        }

        @JsonGetter(FIELD_PARAMETERS)
        public JsonNode parameters() {
            return parameters;
        }

        public Map<String, Field> beforeAndAfterFields() {
            return fields(
                    item -> FIELD_BEFORE.equals(item.field) || FIELD_AFTER.equals(item.field));
        }

        public Map<String, Field> afterFields() {
            return fields(item -> FIELD_AFTER.equals(item.field));
        }

        private Map<String, Field> fields(Predicate<Field> predicate) {
            return fields.stream()
                    .filter(predicate)
                    .flatMap(item -> item.fields.stream())
                    .collect(
                            Collectors.toMap(
                                    Field::field,
                                    Function.identity(),
                                    (v1, v2) -> v2,
                                    LinkedHashMap::new));
        }
    }
}
