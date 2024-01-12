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

package org.apache.paimon.stats;

import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.OptionalUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Col stats, supports the following stats.
 *
 * <ul>
 *   <li>distinctCount: the number of distinct values
 *   <li>min: the minimum value of the column
 *   <li>max: the maximum value of the column
 *   <li>nullCount: the number of nulls
 *   <li>avgLen: average column length
 *   <li>maxLen: max column length
 * </ul>
 */
public class ColStats {

    private static final String FIELD_DISTINCT_COUNT = "distinctCount";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_NULL_COUNT = "nullCount";
    private static final String FIELD_AVG_LEN = "avgLen";
    private static final String FIELD_MAX_LEN = "maxLen";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_DISTINCT_COUNT)
    private final @Nullable Long distinctCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MIN)
    private @Nullable String serializedMin;

    private @Nullable Object min;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX)
    private @Nullable String serializedMax;

    private @Nullable Object max;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NULL_COUNT)
    private final @Nullable Long nullCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_AVG_LEN)
    private final @Nullable Long avgLen;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX_LEN)
    private final @Nullable Long maxLen;

    @JsonCreator
    public ColStats(
            @JsonProperty(FIELD_DISTINCT_COUNT) @Nullable Long distinctCount,
            @JsonProperty(FIELD_MIN) @Nullable String serializedMin,
            @JsonProperty(FIELD_MAX) @Nullable String serializedMax,
            @JsonProperty(FIELD_NULL_COUNT) @Nullable Long nullCount,
            @JsonProperty(FIELD_AVG_LEN) @Nullable Long avgLen,
            @JsonProperty(FIELD_MAX_LEN) @Nullable Long maxLen) {
        this.distinctCount = distinctCount;
        this.serializedMin = serializedMin;
        this.serializedMax = serializedMax;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public ColStats(
            @Nullable Long distinctCount,
            @Nullable Object min,
            @Nullable Object max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public OptionalLong distinctCount() {
        return OptionalUtils.ofNullable(distinctCount);
    }

    public Optional<Object> min() {
        return Optional.ofNullable(min);
    }

    public Optional<Object> max() {
        return Optional.ofNullable(max);
    }

    public OptionalLong nullCount() {
        return OptionalUtils.ofNullable(nullCount);
    }

    public OptionalLong avgLen() {
        return OptionalUtils.ofNullable(avgLen);
    }

    public OptionalLong maxLen() {
        return OptionalUtils.ofNullable(maxLen);
    }

    public void serializeFieldsToString(DataType dataType) {
        if ((min != null && serializedMin == null) || (max != null && serializedMax == null)) {
            Serializer<Object> serializer = InternalSerializers.create(dataType);
            if (min != null && serializedMin == null) {
                serializedMin = serializer.serializeToString(min);
            }
            if (max != null && serializedMax == null) {
                serializedMax = serializer.serializeToString(max);
            }
        }
    }

    public void deserializeFieldsFromString(DataType dataType) {
        if ((serializedMin != null && min == null) || (serializedMax != null && max == null)) {
            Serializer<Object> serializer = InternalSerializers.create(dataType);
            if (serializedMin != null && min == null) {
                min = serializer.deserializeFromString(serializedMin);
            }
            if (serializedMax != null && max == null) {
                max = serializer.deserializeFromString(serializedMax);
            }
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        ColStats colStats = (ColStats) object;
        return Objects.equals(distinctCount, colStats.distinctCount)
                && Objects.equals(serializedMin, colStats.serializedMin)
                && Objects.equals(min, colStats.min)
                && Objects.equals(serializedMax, colStats.serializedMax)
                && Objects.equals(max, colStats.max)
                && Objects.equals(nullCount, colStats.nullCount)
                && Objects.equals(avgLen, colStats.avgLen)
                && Objects.equals(maxLen, colStats.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                distinctCount, serializedMin, min, serializedMax, max, nullCount, avgLen, maxLen);
    }

    @Override
    public String toString() {
        return "ColStats{"
                + "distinctCount="
                + distinctCount
                + ", min='"
                + serializedMin
                + '\''
                + ", max='"
                + serializedMax
                + '\''
                + ", nullCount="
                + nullCount
                + ", avgLen="
                + avgLen
                + ", maxLen="
                + maxLen
                + '}';
    }
}
