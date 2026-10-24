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

import org.apache.paimon.Snapshot;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** Result of acquiring or renewing a commit lease; a null snapshot denotes an empty table. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommitLockResponse implements RESTResponse {

    @JsonProperty("acquired")
    private final boolean acquired;

    @JsonProperty("commitUser")
    @Nullable
    private final String commitUser;

    @JsonProperty("expiresAtMillis")
    private final long expiresAtMillis;

    @JsonProperty("leaseMillis")
    private final long leaseMillis;

    @JsonProperty("snapshot")
    @Nullable
    private final Snapshot snapshot;

    @JsonCreator
    public CommitLockResponse(
            @JsonProperty("acquired") boolean acquired,
            @JsonProperty("commitUser") @Nullable String commitUser,
            @JsonProperty("expiresAtMillis") long expiresAtMillis,
            @JsonProperty("leaseMillis") long leaseMillis,
            @JsonProperty("snapshot") @Nullable Snapshot snapshot) {
        this.acquired = acquired;
        this.commitUser = commitUser;
        this.expiresAtMillis = expiresAtMillis;
        this.leaseMillis = leaseMillis;
        this.snapshot = snapshot;
    }

    public static CommitLockResponse unavailable() {
        return new CommitLockResponse(false, null, 0, 0, null);
    }

    public boolean isAcquired() {
        return acquired;
    }

    @Nullable
    public String getCommitUser() {
        return commitUser;
    }

    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }

    public long getLeaseMillis() {
        return leaseMillis;
    }

    @Nullable
    public Snapshot getSnapshot() {
        return snapshot;
    }
}
