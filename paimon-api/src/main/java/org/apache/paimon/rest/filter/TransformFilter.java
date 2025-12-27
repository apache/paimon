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

package org.apache.paimon.rest.filter;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A REST {@link Filter} that applies a {@link FilterTransform} to a field, then evaluates the
 * transformed result with a {@link LeafFilterFunction} and optional literals.
 *
 * <p>This type is designed for JSON serialization/deserialization in REST requests.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransformFilter implements Filter {

    private static final String FIELD_TRANSFORM = "transform";
    private static final String FIELD_FUNCTION = "function";
    private static final String FIELD_LITERALS = "literals";

    private final FilterTransform transform;
    private final LeafFilterFunction function;
    private final List<Object> literals;

    @JsonCreator
    public TransformFilter(
            @JsonProperty(FIELD_TRANSFORM) FilterTransform transform,
            @JsonProperty(FIELD_FUNCTION) LeafFilterFunction function,
            @JsonProperty(FIELD_LITERALS) List<Object> literals) {
        this.transform = transform;
        this.function = function;
        this.literals = literals;
    }

    @JsonGetter(FIELD_TRANSFORM)
    public FilterTransform transform() {
        return transform;
    }

    @JsonGetter(FIELD_FUNCTION)
    public LeafFilterFunction function() {
        return function;
    }

    @JsonGetter(FIELD_LITERALS)
    public List<Object> literals() {
        return literals;
    }
}
