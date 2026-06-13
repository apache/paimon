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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Response for getting a resource. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GetResourceResponse extends AuditRESTResponse {

    private static final String FIELD_NAME = "name";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_URI = "uri";
    private static final String FIELD_SIZE = "size";
    private static final String FIELD_LAST_MODIFIED_TIME = "lastModifiedTime";
    private static final String FIELD_RESOURCE_TYPE = "resourceType";

    @JsonProperty(FIELD_NAME)
    private final String name;

    @JsonProperty(FIELD_COMMENT)
    private final String comment;

    @JsonProperty(FIELD_URI)
    private final String uri;

    @JsonProperty(FIELD_SIZE)
    private final long size;

    @JsonProperty(FIELD_LAST_MODIFIED_TIME)
    private final long lastModifiedTime;

    @JsonProperty(FIELD_RESOURCE_TYPE)
    private final String resourceType;

    @JsonCreator
    public GetResourceResponse(
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_URI) String uri,
            @JsonProperty(FIELD_SIZE) long size,
            @JsonProperty(FIELD_LAST_MODIFIED_TIME) long lastModifiedTime,
            @JsonProperty(FIELD_RESOURCE_TYPE) String resourceType,
            @JsonProperty(FIELD_OWNER) String owner,
            @JsonProperty(FIELD_CREATED_AT) long createdAt,
            @JsonProperty(FIELD_CREATED_BY) String createdBy,
            @JsonProperty(FIELD_UPDATED_AT) long updatedAt,
            @JsonProperty(FIELD_UPDATED_BY) String updatedBy) {
        super(owner, createdAt, createdBy, updatedAt, updatedBy);
        this.name = name;
        this.comment = comment;
        this.uri = uri;
        this.size = size;
        this.lastModifiedTime = lastModifiedTime;
        this.resourceType = resourceType;
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

    @JsonGetter(FIELD_SIZE)
    public long size() {
        return size;
    }

    @JsonGetter(FIELD_LAST_MODIFIED_TIME)
    public long lastModifiedTime() {
        return lastModifiedTime;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public String resourceType() {
        return resourceType;
    }
}
