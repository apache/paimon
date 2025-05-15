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

package org.apache.paimon.rest.requests;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTRequest;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Request for committing snapshot to table. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitTableRequest implements RESTRequest {

    private static final String FIELD_SNAPSHOT = "snapshot";
    private static final String FIELD_STATISTICS = "statistics";

    @JsonProperty(FIELD_SNAPSHOT)
    private final Snapshot snapshot;

    @JsonProperty(FIELD_STATISTICS)
    private final List<PartitionStatistics> statistics;

    @JsonCreator
    public CommitTableRequest(
            @JsonProperty(FIELD_SNAPSHOT) Snapshot snapshot,
            @JsonProperty(FIELD_STATISTICS) List<PartitionStatistics> statistics) {
        this.snapshot = snapshot;
        this.statistics = statistics;
    }

    @JsonGetter(FIELD_SNAPSHOT)
    public Snapshot getSnapshot() {
        return snapshot;
    }

    @JsonGetter(FIELD_STATISTICS)
    public List<PartitionStatistics> getStatistics() {
        return statistics;
    }
}
