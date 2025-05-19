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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Response for error. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorResponse implements RESTResponse {

    public static final String RESOURCE_TYPE_DATABASE = "DATABASE";

    public static final String RESOURCE_TYPE_TABLE = "TABLE";

    public static final String RESOURCE_TYPE_COLUMN = "COLUMN";

    public static final String RESOURCE_TYPE_SNAPSHOT = "SNAPSHOT";

    public static final String RESOURCE_TYPE_BRANCH = "BRANCH";

    public static final String RESOURCE_TYPE_TAG = "TAG";

    public static final String RESOURCE_TYPE_VIEW = "VIEW";

    public static final String RESOURCE_TYPE_DIALECT = "DIALECT";

    public static final String RESOURCE_TYPE_FUNCTION = "FUNCTION";

    public static final String RESOURCE_TYPE_DEFINITION = "DEFINITION";

    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_RESOURCE_TYPE = "resourceType";
    private static final String FIELD_RESOURCE_NAME = "resourceName";
    private static final String FIELD_CODE = "code";

    @Nullable
    @JsonProperty(FIELD_RESOURCE_TYPE)
    private final String resourceType;

    @Nullable
    @JsonProperty(FIELD_RESOURCE_NAME)
    private final String resourceName;

    @JsonProperty(FIELD_MESSAGE)
    private final String message;

    @JsonProperty(FIELD_CODE)
    private final Integer code;

    @JsonCreator
    public ErrorResponse(
            @Nullable @JsonProperty(FIELD_RESOURCE_TYPE) String resourceType,
            @Nullable @JsonProperty(FIELD_RESOURCE_NAME) String resourceName,
            @JsonProperty(FIELD_MESSAGE) String message,
            @JsonProperty(FIELD_CODE) int code) {
        this.resourceType = resourceType;
        this.resourceName = resourceName;
        this.message = message;
        this.code = code;
    }

    @JsonGetter(FIELD_MESSAGE)
    public String getMessage() {
        return message;
    }

    @JsonGetter(FIELD_RESOURCE_TYPE)
    public String getResourceType() {
        return resourceType;
    }

    @JsonGetter(FIELD_RESOURCE_NAME)
    public String getResourceName() {
        return resourceName;
    }

    @JsonGetter(FIELD_CODE)
    public Integer getCode() {
        return code;
    }
}
