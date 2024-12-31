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

import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.table.Partition;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Partition for rest api. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionResponse implements RESTResponse {

    public static final String FIELD_PARTITION = "partition";

    @JsonProperty(FIELD_PARTITION)
    private final Partition partition;

    @JsonCreator
    public PartitionResponse(@JsonProperty(FIELD_PARTITION) Partition partition) {
        this.partition = partition;
    }

    @JsonGetter(FIELD_PARTITION)
    public Partition getPartition() {
        return partition;
    }
}
