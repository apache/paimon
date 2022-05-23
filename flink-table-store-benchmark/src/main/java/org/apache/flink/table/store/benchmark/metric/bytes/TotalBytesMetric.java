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

package org.apache.flink.table.store.benchmark.metric.bytes;

import org.apache.flink.table.store.benchmark.utils.BenchmarkUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/** Json object of the "numBytesOut" metrics of Flink. */
public class TotalBytesMetric {

    private static final String FIELD_NAME_ID = "id";

    private static final String FIELD_NAME_MIN = "min";

    private static final String FIELD_NAME_MAX = "max";

    private static final String FIELD_NAME_AVG = "avg";

    private static final String FIELD_NAME_SUM = "sum";

    @JsonProperty(value = FIELD_NAME_ID, required = true)
    private final String id;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_MIN)
    private final Long min;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_MAX)
    private final Long max;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_AVG)
    private final Long avg;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_SUM)
    private final Long sum;

    @JsonCreator
    public TotalBytesMetric(
            final @JsonProperty(value = FIELD_NAME_ID, required = true) String id,
            final @Nullable @JsonProperty(FIELD_NAME_MIN) Long min,
            final @Nullable @JsonProperty(FIELD_NAME_MAX) Long max,
            final @Nullable @JsonProperty(FIELD_NAME_AVG) Long avg,
            final @Nullable @JsonProperty(FIELD_NAME_SUM) Long sum) {

        this.id = requireNonNull(id, "id must not be null");
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.sum = sum;
    }

    public TotalBytesMetric(final @JsonProperty(value = FIELD_NAME_ID, required = true) String id) {
        this(id, null, null, null, null);
    }

    @JsonIgnore
    public String getId() {
        return id;
    }

    @JsonIgnore
    public Long getMin() {
        return min;
    }

    @JsonIgnore
    public Long getMax() {
        return max;
    }

    @JsonIgnore
    public Long getSum() {
        return sum;
    }

    @JsonIgnore
    public Long getAvg() {
        return avg;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TotalBytesMetric that = (TotalBytesMetric) o;
        return Objects.equals(id, that.id)
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(avg, that.avg)
                && Objects.equals(sum, that.sum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, min, max, avg, sum);
    }

    @Override
    public String toString() {
        return "TotalBytesMetric{"
                + "id='"
                + id
                + '\''
                + ", mim='"
                + min
                + '\''
                + ", max='"
                + max
                + '\''
                + ", avg='"
                + avg
                + '\''
                + ", sum='"
                + sum
                + '\''
                + '}';
    }

    public static TotalBytesMetric fromJson(String json) {
        try {
            JsonNode jsonNode = BenchmarkUtils.MAPPER.readTree(json);
            return BenchmarkUtils.MAPPER.convertValue(jsonNode.get(0), TotalBytesMetric.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
