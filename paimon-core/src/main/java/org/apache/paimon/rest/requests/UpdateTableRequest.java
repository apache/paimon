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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for updating table. */
public class UpdateTableRequest implements RESTRequest {

    private static final String FIELD_FROM_IDENTIFIER_NAME = "from";
    private static final String FIELD_TO_IDENTIFIER_NAME = "to";
    private static final String FIELD_SCHEMA_CHANGES_NAME = "changes";

    @JsonProperty(FIELD_FROM_IDENTIFIER_NAME)
    private Identifier fromIdentifier;

    @JsonProperty(FIELD_TO_IDENTIFIER_NAME)
    private Identifier toIdentifier;

    @JsonProperty(FIELD_SCHEMA_CHANGES_NAME)
    private SchemaChanges changes;

    @JsonCreator
    public UpdateTableRequest(
            @JsonProperty(FIELD_FROM_IDENTIFIER_NAME) Identifier fromIdentifier,
            @JsonProperty(FIELD_TO_IDENTIFIER_NAME) Identifier toIdentifier,
            @JsonProperty(FIELD_SCHEMA_CHANGES_NAME) SchemaChanges changes) {
        this.fromIdentifier = fromIdentifier;
        this.toIdentifier = toIdentifier;
        this.changes = changes;
    }

    @JsonGetter(FIELD_FROM_IDENTIFIER_NAME)
    public Identifier getFromIdentifier() {
        return fromIdentifier;
    }

    @JsonGetter(FIELD_TO_IDENTIFIER_NAME)
    public Identifier getToIdentifier() {
        return toIdentifier;
    }

    @JsonGetter(FIELD_SCHEMA_CHANGES_NAME)
    public SchemaChanges getChanges() {
        return changes;
    }
}
