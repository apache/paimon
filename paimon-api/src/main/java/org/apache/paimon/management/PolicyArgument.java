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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.beans.ConstructorProperties;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Positional column or constant argument passed to a data policy function. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class PolicyArgument {

    private static final String FIELD_COLUMN = "column";
    private static final String FIELD_CONSTANT = "constant";

    @Nullable
    @JsonProperty(FIELD_COLUMN)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String column;

    @Nullable
    @JsonProperty(FIELD_CONSTANT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final String constant;

    @JsonCreator
    @ConstructorProperties({FIELD_COLUMN, FIELD_CONSTANT})
    public PolicyArgument(
            @Nullable @JsonProperty(FIELD_COLUMN) String column,
            @Nullable @JsonProperty(FIELD_CONSTANT) String constant) {
        boolean hasColumn = !isBlank(column);
        boolean hasConstant = constant != null;
        checkArgument(
                hasColumn != hasConstant,
                "A policy argument must contain exactly one of column and constant.");
        this.column = hasColumn ? column : null;
        this.constant = hasConstant ? constant : null;
    }

    public static PolicyArgument column(String column) {
        return new PolicyArgument(column, null);
    }

    public static PolicyArgument constant(String constant) {
        return new PolicyArgument(null, constant);
    }

    @Nullable
    @JsonGetter(FIELD_COLUMN)
    public String getColumn() {
        return column;
    }

    @Nullable
    @JsonGetter(FIELD_CONSTANT)
    public String getConstant() {
        return constant;
    }

    private static boolean isBlank(@Nullable String value) {
        return value == null || value.trim().isEmpty();
    }
}
