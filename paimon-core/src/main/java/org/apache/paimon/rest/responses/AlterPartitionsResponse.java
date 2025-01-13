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

package org.apache.paimon.rest.responses;

import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** response for altering partitions. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlterPartitionsResponse implements RESTResponse {

    public static final String FIELD_SUCCESS_PARTITIONS = "successPartitions";
    public static final String FIELD_FAIL_PARTITIONS = "failPartitions";

    @JsonProperty(FIELD_SUCCESS_PARTITIONS)
    private final List<Partition> successPartitions;

    @JsonProperty(FIELD_FAIL_PARTITIONS)
    private final List<Partition> failPartitions;

    @JsonCreator
    public AlterPartitionsResponse(
            @JsonProperty(FIELD_SUCCESS_PARTITIONS) List<Partition> successPartitions,
            @JsonProperty(FIELD_FAIL_PARTITIONS) List<Partition> failPartitions) {
        this.successPartitions = successPartitions;
        this.failPartitions = failPartitions;
    }

    @JsonGetter(FIELD_SUCCESS_PARTITIONS)
    public List<Partition> getSuccessPartitions() {
        return successPartitions;
    }

    @JsonGetter(FIELD_FAIL_PARTITIONS)
    public List<Partition> getFailPartitions() {
        return failPartitions;
    }
}
