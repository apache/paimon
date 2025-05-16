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

package org.apache.paimon.rest.requests;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.schema.Schema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for creating table. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateTableRequest implements RESTRequest {

    private static final String FIELD_IDENTIFIER = "identifier";
    private static final String FIELD_SCHEMA = "schema";

    @JsonProperty(FIELD_IDENTIFIER)
    private final Identifier identifier;

    @JsonProperty(FIELD_SCHEMA)
    private final Schema schema;

    @JsonCreator
    public CreateTableRequest(
            @JsonProperty(FIELD_IDENTIFIER) Identifier identifier,
            @JsonProperty(FIELD_SCHEMA) Schema schema) {
        this.schema = schema;
        this.identifier = identifier;
    }

    @JsonGetter(FIELD_IDENTIFIER)
    public Identifier getIdentifier() {
        return identifier;
    }

    @JsonGetter(FIELD_SCHEMA)
    public Schema getSchema() {
        return schema;
    }
}
