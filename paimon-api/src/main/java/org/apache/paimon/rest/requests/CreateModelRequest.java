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

import org.apache.paimon.catalog.Model;
import org.apache.paimon.catalog.ModelDefinition;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Request for creating database-scoped model metadata. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateModelRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_DEFINITION = "definition";

    private final String name;
    private final @Nullable String comment;
    private final ModelDefinition definition;

    @JsonCreator
    public CreateModelRequest(
            @JsonProperty(FIELD_NAME) String name,
            @Nullable @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_DEFINITION) ModelDefinition definition) {
        checkArgument(name != null && !name.trim().isEmpty(), "name must not be blank");
        checkArgument(comment == null || !comment.trim().isEmpty(), "comment must not be blank");
        checkArgument(definition != null, "definition must not be null");
        this.name = name;
        this.comment = comment;
        this.definition = definition;
    }

    public CreateModelRequest(Model model) {
        this(model.name(), model.comment(), model.definition());
    }

    @JsonGetter(FIELD_NAME)
    public String getName() {
        return name;
    }

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonGetter(FIELD_COMMENT)
    public String getComment() {
        return comment;
    }

    @JsonGetter(FIELD_DEFINITION)
    public ModelDefinition getDefinition() {
        return definition;
    }
}
