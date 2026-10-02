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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;

/** A label bound directly to one entity in the configured REST catalog prefix. */
@Experimental
public class GetLabelResponse implements RESTResponse {

    private static final String FIELD_ENTITY_TYPE = "entityType";
    private static final String FIELD_ENTITY_NAME = "entityName";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";

    @JsonProperty(FIELD_ENTITY_TYPE)
    private final String entityType;

    @JsonProperty(FIELD_ENTITY_NAME)
    private final String entityName;

    @JsonProperty(FIELD_KEY)
    private final String key;

    @JsonProperty(FIELD_VALUE)
    private final String value;

    @JsonCreator
    @ConstructorProperties({FIELD_ENTITY_TYPE, FIELD_ENTITY_NAME, FIELD_KEY, FIELD_VALUE})
    public GetLabelResponse(
            @JsonProperty(FIELD_ENTITY_TYPE) String entityType,
            @JsonProperty(FIELD_ENTITY_NAME) String entityName,
            @JsonProperty(FIELD_KEY) String key,
            @JsonProperty(FIELD_VALUE) String value) {
        this.entityType = entityType;
        this.entityName = entityName;
        this.key = key;
        this.value = value;
    }

    @JsonGetter(FIELD_ENTITY_TYPE)
    public String getEntityType() {
        return entityType;
    }

    @JsonGetter(FIELD_ENTITY_NAME)
    public String getEntityName() {
        return entityName;
    }

    @JsonGetter(FIELD_KEY)
    public String getKey() {
        return key;
    }

    @JsonGetter(FIELD_VALUE)
    public String getValue() {
        return value;
    }
}
