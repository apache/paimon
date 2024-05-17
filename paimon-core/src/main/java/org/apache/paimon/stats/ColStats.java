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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.OptionalUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
 *
 * @param <T> col internal data type
 */
@Experimental
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColStats<T> {

    private static final String FIELD_COL_ID = "colId";
    private static final String FIELD_DISTINCT_COUNT = "distinctCount";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_NULL_COUNT = "nullCount";
    private static final String FIELD_AVG_LEN = "avgLen";
    private static final String FIELD_MAX_LEN = "maxLen";

    @JsonProperty(FIELD_COL_ID)
    private final int colId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_DISTINCT_COUNT)
    private final @Nullable Long distinctCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MIN)
    private @Nullable String serializedMin;

    private @Nullable Comparable<T> min;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX)
    private @Nullable String serializedMax;

    private @Nullable Comparable<T> max;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NULL_COUNT)
    private final @Nullable Long nullCount;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_AVG_LEN)
    private final @Nullable Long avgLen;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_MAX_LEN)
    private final @Nullable Long maxLen;

    // This should only be used by jackson
    @JsonCreator
    public ColStats(
            @JsonProperty(FIELD_COL_ID) int colId,
            @JsonProperty(FIELD_DISTINCT_COUNT) @Nullable Long distinctCount,
            @JsonProperty(FIELD_MIN) @Nullable String serializedMin,
            @JsonProperty(FIELD_MAX) @Nullable String serializedMax,
            @JsonProperty(FIELD_NULL_COUNT) @Nullable Long nullCount,
            @JsonProperty(FIELD_AVG_LEN) @Nullable Long avgLen,
            @JsonProperty(FIELD_MAX_LEN) @Nullable Long maxLen) {
        this.colId = colId;
        this.distinctCount = distinctCount;
        this.serializedMin = serializedMin;
        this.serializedMax = serializedMax;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    private ColStats(
            int colId,
            @Nullable Long distinctCount,
            @Nullable Comparable<T> min,
            @Nullable Comparable<T> max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        this.colId = colId;
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
    }

    public static <T> ColStats<T> newColStats(
            int colId,
            @Nullable Long distinctCount,
            @Nullable Comparable<T> min,
            @Nullable Comparable<T> max,
            @Nullable Long nullCount,
            @Nullable Long avgLen,
            @Nullable Long maxLen) {
        return new ColStats<>(colId, distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    public int colId() {
        return colId;
    }

    public OptionalLong distinctCount() {
        return OptionalUtils.ofNullable(distinctCount);
    }

    public Optional<Comparable<T>> min() {
        return Optional.ofNullable(min);
    }

    public Optional<Comparable<T>> max() {
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

    @SuppressWarnings("unchecked")
    public void serializeFieldsToString(DataType dataType) {
        if ((min != null && serializedMin == null) || (max != null && serializedMax == null)) {
            Serializer<T> serializer = InternalSerializers.create(dataType);
            if (min != null && serializedMin == null) {
                serializedMin = serializer.serializeToString((T) min);
            }
            if (max != null && serializedMax == null) {
                serializedMax = serializer.serializeToString((T) max);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void deserializeFieldsFromString(DataType dataType) {
        if ((serializedMin != null && min == null) || (serializedMax != null && max == null)) {
            Serializer<T> serializer = InternalSerializers.create(dataType);
            if (serializedMin != null && min == null) {
                min = (Comparable<T>) serializer.deserializeFromString(serializedMin);
            }
            if (serializedMax != null && max == null) {
                max = (Comparable<T>) serializer.deserializeFromString(serializedMax);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColStats<?> colStats = (ColStats<?>) o;
        return colId == colStats.colId
                && Objects.equals(distinctCount, colStats.distinctCount)
                && Objects.equals(min, colStats.min)
                && Objects.equals(max, colStats.max)
                && Objects.equals(nullCount, colStats.nullCount)
                && Objects.equals(avgLen, colStats.avgLen)
                && Objects.equals(maxLen, colStats.maxLen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(colId, distinctCount, min, max, nullCount, avgLen, maxLen);
    }

    @Override
    public String toString() {
        return JsonSerdeUtil.toJson(this);
    }
}
