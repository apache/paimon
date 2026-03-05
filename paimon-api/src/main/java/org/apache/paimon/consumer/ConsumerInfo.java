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

package org.apache.paimon.consumer;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Entry representing a consumer with id and nextSnapshot. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerInfo {
    private static final String FIELD_CONSUMER_ID = "consumerId";
    private static final String FIELD_NEXT_SNAPSHOT = "nextSnapshot";

    @JsonProperty(FIELD_CONSUMER_ID)
    private final String consumerId;

    @JsonProperty(FIELD_NEXT_SNAPSHOT)
    private final Long nextSnapshot;

    @JsonCreator
    public ConsumerInfo(
            @JsonProperty(FIELD_CONSUMER_ID) String consumerId,
            @JsonProperty(FIELD_NEXT_SNAPSHOT) Long nextSnapshot) {
        this.consumerId = consumerId;
        this.nextSnapshot = nextSnapshot;
    }

    @JsonGetter(FIELD_CONSUMER_ID)
    public String getConsumerId() {
        return consumerId;
    }

    @JsonGetter(FIELD_NEXT_SNAPSHOT)
    public Long getNextSnapshot() {
        return nextSnapshot;
    }
}
