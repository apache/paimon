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

/** Column-mask expression and its optional server-compiled transform. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMask {

    private static final String FIELD_EXPRESSION = "expression";
    private static final String FIELD_TRANSFORM = "transform";

    @Nullable
    @JsonProperty(FIELD_EXPRESSION)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String expression;

    @Nullable
    @JsonProperty(FIELD_TRANSFORM)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String transform;

    @JsonCreator
    @ConstructorProperties({FIELD_EXPRESSION, FIELD_TRANSFORM})
    public ColumnMask(
            @Nullable @JsonProperty(FIELD_EXPRESSION) String expression,
            @Nullable @JsonProperty(FIELD_TRANSFORM) String transform) {
        this.expression = expression;
        this.transform = transform;
    }

    @Nullable
    @JsonGetter(FIELD_EXPRESSION)
    public String getExpression() {
        return expression;
    }

    @Nullable
    @JsonGetter(FIELD_TRANSFORM)
    public String getTransform() {
        return transform;
    }
}
