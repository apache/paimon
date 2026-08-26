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

/** Protected column and serialized Paimon transform for a column-mask policy. */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMask {

    public static final int MAX_TRANSFORM_BYTES = 60 * 1024;

    private static final String FIELD_ON_COLUMN = "onColumn";
    private static final String FIELD_TRANSFORM = "transform";

    @JsonProperty(FIELD_ON_COLUMN)
    private final String onColumn;

    @JsonProperty(FIELD_TRANSFORM)
    private final String transform;

    @JsonCreator
    @ConstructorProperties({FIELD_ON_COLUMN, FIELD_TRANSFORM})
    public ColumnMask(
            @JsonProperty(FIELD_ON_COLUMN) String onColumn,
            @JsonProperty(FIELD_TRANSFORM) String transform) {
        checkArgument(!isBlank(onColumn), "onColumn cannot be empty.");
        checkArgument(!isBlank(transform), "transform cannot be empty.");
        checkArgument(
                transform.getBytes(StandardCharsets.UTF_8).length <= MAX_TRANSFORM_BYTES,
                "transform must not exceed %s UTF-8 bytes.",
                MAX_TRANSFORM_BYTES);
        this.onColumn = onColumn;
        this.transform = transform;
    }

    @JsonGetter(FIELD_ON_COLUMN)
    public String getOnColumn() {
        return onColumn;
    }

    @JsonGetter(FIELD_TRANSFORM)
    public String getTransform() {
        return transform;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
