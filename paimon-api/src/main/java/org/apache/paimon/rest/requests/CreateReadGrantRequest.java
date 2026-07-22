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
import org.apache.paimon.catalog.ReadAuthorizationResource;
import org.apache.paimon.catalog.ReadAuthorizationRootType;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Request for authorizing candidate dependency targets of a table or view read. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateReadGrantRequest implements RESTRequest {

    private static final String FIELD_ROOT_TYPE = "rootType";
    private static final String FIELD_ROOT_IDENTIFIER = "rootIdentifier";
    private static final String FIELD_TARGETS = "targets";
    private static final String FIELD_PREVIOUS_READ_GRANT = "previousReadGrant";

    private final ReadAuthorizationRootType rootType;
    private final Identifier rootIdentifier;
    private final List<ReadAuthorizationResource> targets;
    @Nullable private final String previousReadGrant;

    @JsonCreator
    public CreateReadGrantRequest(
            @JsonProperty(FIELD_ROOT_TYPE) ReadAuthorizationRootType rootType,
            @JsonProperty(FIELD_ROOT_IDENTIFIER) Identifier rootIdentifier,
            @JsonProperty(FIELD_TARGETS) List<ReadAuthorizationResource> targets,
            @Nullable @JsonProperty(FIELD_PREVIOUS_READ_GRANT) String previousReadGrant) {
        this.rootType = Objects.requireNonNull(rootType, "rootType");
        this.rootIdentifier = Objects.requireNonNull(rootIdentifier, "rootIdentifier");
        this.targets =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(targets, "targets")));
        this.previousReadGrant = previousReadGrant;
    }

    @JsonGetter(FIELD_ROOT_TYPE)
    public ReadAuthorizationRootType rootType() {
        return rootType;
    }

    @JsonGetter(FIELD_ROOT_IDENTIFIER)
    public Identifier rootIdentifier() {
        return rootIdentifier;
    }

    @JsonGetter(FIELD_TARGETS)
    public List<ReadAuthorizationResource> targets() {
        return targets;
    }

    @Nullable
    @JsonGetter(FIELD_PREVIOUS_READ_GRANT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String previousReadGrant() {
        return previousReadGrant;
    }
}
