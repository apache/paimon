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
import org.apache.paimon.view.SemanticViewDefinition;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Complete definition replacement, optionally conditioned on an existing revision. */
@Experimental
public class UpsertSemanticViewRequest implements RESTRequest {

    private final SemanticViewDefinition definition;
    @Nullable private final String expectedRevision;

    @JsonCreator
    @ConstructorProperties({"definition", "expectedRevision"})
    public UpsertSemanticViewRequest(
            @JsonProperty("definition") SemanticViewDefinition definition,
            @Nullable @JsonProperty("expectedRevision") String expectedRevision) {
        checkArgument(definition != null, "definition must not be null");
        checkArgument(
                expectedRevision == null || !expectedRevision.trim().isEmpty(),
                "expectedRevision must not be blank");
        this.definition = definition;
        this.expectedRevision = expectedRevision;
    }

    @JsonGetter("definition")
    public SemanticViewDefinition getDefinition() {
        return definition;
    }

    @Nullable
    @JsonGetter("expectedRevision")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getExpectedRevision() {
        return expectedRevision;
    }

    @Override
    @JsonIgnore
    public boolean isRetrySafe() {
        // A successful conditional write consumes its revision; replay can mask that success.
        return expectedRevision == null;
    }
}
