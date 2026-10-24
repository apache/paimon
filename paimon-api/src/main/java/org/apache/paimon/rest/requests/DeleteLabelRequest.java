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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;

import static org.apache.paimon.rest.requests.LabelRequests.requireNonBlank;

/** Label binding to remove, identified entirely by the request body. */
@Experimental
public class DeleteLabelRequest implements RESTRequest {

    private static final String FIELD_ENTITY_TYPE = "entityType";
    private static final String FIELD_ENTITY_NAME = "entityName";
    private static final String FIELD_KEY = "key";

    @JsonProperty(FIELD_ENTITY_TYPE)
    private final String entityType;

    @JsonProperty(FIELD_ENTITY_NAME)
    private final String entityName;

    @JsonProperty(FIELD_KEY)
    private final String key;

    @JsonCreator
    @ConstructorProperties({FIELD_ENTITY_TYPE, FIELD_ENTITY_NAME, FIELD_KEY})
    public DeleteLabelRequest(
            @JsonProperty(FIELD_ENTITY_TYPE) String entityType,
            @JsonProperty(FIELD_ENTITY_NAME) String entityName,
            @JsonProperty(FIELD_KEY) String key) {
        this.entityType = requireNonBlank(entityType, FIELD_ENTITY_TYPE);
        this.entityName = requireNonBlank(entityName, FIELD_ENTITY_NAME);
        this.key = requireNonBlank(key, FIELD_KEY);
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
}
