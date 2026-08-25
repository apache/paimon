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

package org.apache.paimon.management;

import org.apache.paimon.annotation.Experimental;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Function and positional arguments for a row-filter policy. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class RowFilter {

    private static final String FIELD_FUNCTION_NAME = "functionName";
    private static final String FIELD_FUNCTION_ARGUMENTS = "functionArguments";

    @JsonProperty(FIELD_FUNCTION_NAME)
    private final String functionName;

    @JsonProperty(FIELD_FUNCTION_ARGUMENTS)
    private final List<PolicyArgument> functionArguments;

    @JsonCreator
    @ConstructorProperties({FIELD_FUNCTION_NAME, FIELD_FUNCTION_ARGUMENTS})
    public RowFilter(
            @JsonProperty(FIELD_FUNCTION_NAME) String functionName,
            @Nullable @JsonProperty(FIELD_FUNCTION_ARGUMENTS)
                    List<PolicyArgument> functionArguments) {
        checkArgument(!isBlank(functionName), "functionName cannot be empty.");
        this.functionName = functionName;
        this.functionArguments = immutable(functionArguments);
    }

    @JsonGetter(FIELD_FUNCTION_NAME)
    public String getFunctionName() {
        return functionName;
    }

    @JsonGetter(FIELD_FUNCTION_ARGUMENTS)
    public List<PolicyArgument> getFunctionArguments() {
        return functionArguments;
    }

    private static List<PolicyArgument> immutable(@Nullable List<PolicyArgument> arguments) {
        List<PolicyArgument> result =
                arguments == null ? Collections.emptyList() : new ArrayList<>(arguments);
        checkArgument(!result.contains(null), "functionArguments cannot contain null arguments.");
        return Collections.unmodifiableList(result);
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
