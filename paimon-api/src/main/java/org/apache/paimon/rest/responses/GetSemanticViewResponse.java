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
import org.apache.paimon.view.SemanticViewDefinition;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.beans.ConstructorProperties;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A committed semantic view definition and its opaque revision. */
@Experimental
public class GetSemanticViewResponse implements RESTResponse {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_ENTITY_NAME = "entityName";
    private static final String FIELD_DEFINITION = "definition";
    private static final String FIELD_REVISION = "revision";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_ENTITY_NAME)
    private final String entityName;

    @JsonProperty(FIELD_DEFINITION)
    private final SemanticViewDefinition definition;

    @JsonProperty(FIELD_REVISION)
    private final String revision;

    @JsonCreator
    @ConstructorProperties({FIELD_NAME, FIELD_ENTITY_NAME, FIELD_DEFINITION, FIELD_REVISION})
    public GetSemanticViewResponse(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_ENTITY_NAME) String entityName,
            @JsonProperty(FIELD_DEFINITION) SemanticViewDefinition definition,
            @JsonProperty(FIELD_REVISION) String revision) {
        checkArgument(name != null && !name.trim().isEmpty(), "name must not be blank");
        checkArgument(
                entityName != null && !entityName.trim().isEmpty(), "entityName must not be blank");
        checkArgument(definition != null, "definition must not be null");
        // A missing revision must never turn a subsequent conditional write into an upsert.
        checkArgument(revision != null && !revision.trim().isEmpty(), "revision must not be blank");
        this.name = name;
        this.entityName = entityName;
        this.definition = definition;
        this.revision = revision;
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @JsonGetter(FIELD_ENTITY_NAME)
    public String getEntityName() {
        return entityName;
    }

    @JsonGetter(FIELD_DEFINITION)
    public SemanticViewDefinition getDefinition() {
        return definition;
    }

    @JsonGetter(FIELD_REVISION)
    public String getRevision() {
        return revision;
    }
}
