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

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Base class for database, table, view, audit response. */
public abstract class BaseResourceAuditResponse implements RESTResponse {
    protected static final String FIELD_OWNER = "owner";
    protected static final String FIELD_CREATED_AT = "createdAt";
    protected static final String FIELD_CREATED_BY = "createdBy";
    protected static final String FIELD_UPDATED_AT = "updatedAt";
    protected static final String FIELD_UPDATED_BY = "updatedBy";

    @JsonProperty(FIELD_OWNER)
    private final String owner;

    @JsonProperty(FIELD_CREATED_AT)
    private final long createdAt;

    @JsonProperty(FIELD_CREATED_BY)
    private final String createdBy;

    @JsonProperty(FIELD_UPDATED_AT)
    private final long updatedAt;

    @JsonProperty(FIELD_UPDATED_BY)
    private final String updatedBy;

    @JsonCreator
    public BaseResourceAuditResponse(
            @JsonProperty(FIELD_OWNER) String owner,
            @JsonProperty(FIELD_CREATED_AT) long createdAt,
            @JsonProperty(FIELD_CREATED_BY) String createdBy,
            @JsonProperty(FIELD_UPDATED_AT) long updatedAt,
            @JsonProperty(FIELD_UPDATED_BY) String updatedBy) {
        this.owner = owner;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.updatedAt = updatedAt;
        this.updatedBy = updatedBy;
    }

    @JsonGetter(FIELD_OWNER)
    public String getOwner() {
        return owner;
    }

    @JsonGetter(FIELD_CREATED_AT)
    public long getCreatedAt() {
        return createdAt;
    }

    @JsonGetter(FIELD_CREATED_BY)
    public String getCreatedBy() {
        return createdBy;
    }

    @JsonGetter(FIELD_UPDATED_AT)
    public long getUpdatedAt() {
        return updatedAt;
    }

    @JsonGetter(FIELD_UPDATED_BY)
    public String getUpdatedBy() {
        return updatedBy;
    }
}
