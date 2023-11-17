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

package org.apache.paimon.benchmark.metric.cpu;

import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CpuMetric {

    private static final String FIELD_NAME_HOST = "host";
    private static final String FIELD_NAME_PID = "pid";
    private static final String FIELD_NAME_CPU = "cpu";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(value = FIELD_NAME_HOST, required = true)
    private final String host;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(value = FIELD_NAME_PID, required = true)
    private final int pid;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(value = FIELD_NAME_CPU, required = true)
    private final double cpu;

    @JsonCreator
    public CpuMetric(
            @Nullable @JsonProperty(FIELD_NAME_HOST) String host,
            @Nullable @JsonProperty(FIELD_NAME_PID) int pid,
            @JsonProperty(FIELD_NAME_CPU) double cpu) {
        this.host = host;
        this.pid = pid;
        this.cpu = cpu;
    }

    @JsonIgnore
    public String getHost() {
        return host;
    }

    @JsonIgnore
    public int getPid() {
        return pid;
    }

    @JsonIgnore
    public double getCpu() {
        return cpu;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CpuMetric that = (CpuMetric) o;
        return Double.compare(that.cpu, cpu) == 0
                && Objects.equals(host, that.host)
                && Objects.equals(pid, that.pid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, pid, cpu);
    }

    @Override
    public String toString() {
        try {
            return BenchmarkUtils.JSON_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<CpuMetric> fromJsonArray(String json) {
        try {
            ArrayNode arrayNode = (ArrayNode) BenchmarkUtils.JSON_MAPPER.readTree(json);
            List<CpuMetric> expected = new ArrayList<>();
            for (JsonNode jsonNode : arrayNode) {
                expected.add(BenchmarkUtils.JSON_MAPPER.convertValue(jsonNode, CpuMetric.class));
            }
            return expected;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
