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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

/** Row-filter expression and its optional server-compiled predicate. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RowFilter {

    private static final String FIELD_EXPRESSION = "expression";
    private static final String FIELD_PREDICATE = "predicate";

    @Nullable
    @JsonProperty(FIELD_EXPRESSION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String expression;

    @Nullable
    @JsonProperty(FIELD_PREDICATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String predicate;

    @JsonCreator
    @ConstructorProperties({FIELD_EXPRESSION, FIELD_PREDICATE})
    public RowFilter(
            @Nullable @JsonProperty(FIELD_EXPRESSION) String expression,
            @Nullable @JsonProperty(FIELD_PREDICATE) String predicate) {
        this.expression = expression;
        this.predicate = predicate;
    }

    @Nullable
    @JsonGetter(FIELD_EXPRESSION)
    public String getExpression() {
        return expression;
    }

    @Nullable
    @JsonGetter(FIELD_PREDICATE)
    public String getPredicate() {
        return predicate;
    }
}
