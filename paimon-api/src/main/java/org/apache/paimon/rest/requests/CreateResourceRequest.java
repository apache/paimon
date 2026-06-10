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

import org.apache.paimon.resource.Resource;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Request for creating resource. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateResourceRequest implements RESTRequest {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_URI = "uri";
    private static final String FIELD_RESOURCE_TYPE = "resourceType";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_COMMENT)
    private final String comment;

    @JsonProperty(FIELD_URI)
    private final String uri;

    @JsonProperty(FIELD_RESOURCE_TYPE)
    private final String resourceType;

    @JsonCreator
    public CreateResourceRequest(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_URI) String uri,
            @JsonProperty(FIELD_RESOURCE_TYPE) String resourceType) {
        this.name = name;
        this.comment = comment;
        this.uri = uri;
        this.resourceType = resourceType;
    }

    public CreateResourceRequest(Resource resource) {
        this.name = resource.name();
        this.comment = resource.comment().orElse(null);
        this.uri = resource.uri();
        this.resourceType = resource.resourceType().getValue();
    }

    @JsonGetter(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonGetter(FIELD_COMMENT)
    public String comment() {
        return comment;
    }

    @JsonGetter(FIELD_URI)
    public String uri() {
        return uri;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public String resourceType() {
        return resourceType;
    }
}
