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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response for listing or getting table schemas. All schema queries (latest / earliest / by id / by
 * range / list all) return this shape; the server is responsible for filtering.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListSchemaResponse implements RESTResponse {

    private static final String FIELD_SCHEMAS = "schemas";

    @JsonProperty(FIELD_SCHEMAS)
    private final List<SchemaItem> schemas;

    @JsonCreator
    public ListSchemaResponse(@JsonProperty(FIELD_SCHEMAS) List<SchemaItem> schemas) {
        this.schemas = schemas;
    }

    @JsonGetter(FIELD_SCHEMAS)
    public List<SchemaItem> getSchemas() {
        return schemas;
    }

    /** One schema entry in a {@link ListSchemaResponse}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SchemaItem {

        private static final String FIELD_SCHEMA_ID = "schemaId";
        private static final String FIELD_SCHEMA = "schema";
        private static final String FIELD_CREATED_AT = "createdAt";

        @JsonProperty(FIELD_SCHEMA_ID)
        private final long schemaId;

        @JsonProperty(FIELD_SCHEMA)
        private final Schema schema;

        @JsonProperty(FIELD_CREATED_AT)
        private final long createdAt;

        @JsonCreator
        public SchemaItem(
                @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
                @JsonProperty(FIELD_SCHEMA) Schema schema,
                @JsonProperty(FIELD_CREATED_AT) long createdAt) {
            this.schemaId = schemaId;
            this.schema = schema;
            this.createdAt = createdAt;
        }

        @JsonGetter(FIELD_SCHEMA_ID)
        public long getSchemaId() {
            return schemaId;
        }

        @JsonGetter(FIELD_SCHEMA)
        public Schema getSchema() {
            return schema;
        }

        @JsonGetter(FIELD_CREATED_AT)
        public long getCreatedAt() {
            return createdAt;
        }
    }
}
