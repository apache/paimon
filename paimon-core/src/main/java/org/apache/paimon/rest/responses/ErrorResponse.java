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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Response for error. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorResponse implements RESTResponse {

    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_CODE = "code";
    private static final String FIELD_STACK = "stack";

    @JsonProperty(FIELD_MESSAGE)
    private final String message;

    @JsonProperty(FIELD_CODE)
    private final Integer code;

    @JsonProperty(FIELD_STACK)
    private final List<String> stack;

    public ErrorResponse(String message, Integer code) {
        this.code = code;
        this.message = message;
        this.stack = new ArrayList<String>();
    }

    @JsonCreator
    public ErrorResponse(
            @JsonProperty(FIELD_MESSAGE) String message,
            @JsonProperty(FIELD_CODE) int code,
            @JsonProperty(FIELD_STACK) List<String> stack) {
        this.message = message;
        this.code = code;
        this.stack = stack;
    }

    public ErrorResponse(String message, int code, Throwable throwable) {
        this.message = message;
        this.code = code;
        this.stack = getStackFromThrowable(throwable);
    }

    @JsonGetter(FIELD_MESSAGE)
    public String getMessage() {
        return message;
    }

    @JsonGetter(FIELD_CODE)
    public Integer getCode() {
        return code;
    }

    @JsonGetter(FIELD_STACK)
    public List<String> getStack() {
        return stack;
    }

    private List<String> getStackFromThrowable(Throwable throwable) {
        if (throwable == null) {
            return new ArrayList<String>();
        }
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            throwable.printStackTrace(pw);
        }

        return Arrays.asList(sw.toString().split("\n"));
    }
}
