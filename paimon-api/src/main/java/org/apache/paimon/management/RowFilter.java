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

import java.beans.ConstructorProperties;
import java.nio.charset.StandardCharsets;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Serialized Paimon predicate for a row-filter policy. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class RowFilter {

    public static final int MAX_PREDICATE_BYTES = 60 * 1024;

    private static final String FIELD_PREDICATE = "predicate";

    @JsonProperty(FIELD_PREDICATE)
    private final String predicate;

    @JsonCreator
    @ConstructorProperties({FIELD_PREDICATE})
    public RowFilter(@JsonProperty(FIELD_PREDICATE) String predicate) {
        checkArgument(!isBlank(predicate), "predicate cannot be empty.");
        checkArgument(
                predicate.getBytes(StandardCharsets.UTF_8).length <= MAX_PREDICATE_BYTES,
                "predicate must not exceed %s UTF-8 bytes.",
                MAX_PREDICATE_BYTES);
        this.predicate = predicate;
    }

    @JsonGetter(FIELD_PREDICATE)
    public String getPredicate() {
        return predicate;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
