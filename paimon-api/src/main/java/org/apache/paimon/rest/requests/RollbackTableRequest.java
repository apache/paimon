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

import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Request for rollback table. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RollbackTableRequest implements RESTRequest {

    private static final String FIELD_INSTANT = "instant";
    private static final String FIELD_FROM_SNAPSHOT = "fromSnapshot";

    @JsonProperty(FIELD_INSTANT)
    private final Instant instant;

    @JsonProperty(FIELD_FROM_SNAPSHOT)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Long fromSnapshot;

    @JsonCreator
    public RollbackTableRequest(
            @JsonProperty(FIELD_INSTANT) Instant instant,
            @JsonProperty(FIELD_FROM_SNAPSHOT) @Nullable Long fromSnapshot) {
        this.instant = instant;
        this.fromSnapshot = fromSnapshot;
    }

    @JsonGetter(FIELD_INSTANT)
    public Instant getInstant() {
        return instant;
    }

    @JsonGetter(FIELD_FROM_SNAPSHOT)
    @Nullable
    public Long getFromSnapshot() {
        return fromSnapshot;
    }
}
