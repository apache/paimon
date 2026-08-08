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

package org.apache.paimon.service;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * Versioned discovery descriptor for one materialized global-index generation.
 *
 * <p>For a ready descriptor, {@code servedGeneration}, {@code servedSnapshotId}, and {@code
 * snapshotUuid} fence the snapshot accepted by the endpoints. For a not-ready descriptor, the
 * generation and snapshot identify the target which has been fenced and acknowledged; no snapshot
 * is served and the endpoint set is empty.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class GlobalIndexQueryServiceDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int PROTOCOL_VERSION = 2;
    public static final int KEY_HASH_VERSION = 1;
    public static final String LAYOUT = "key-hash-materialized-v1";

    @JsonProperty("protocolVersion")
    private final int protocolVersion;

    @JsonProperty("tableUuid")
    private final String tableUuid;

    @JsonProperty("branch")
    private final String branch;

    @JsonProperty("schemaId")
    private final long schemaId;

    @JsonProperty("schemaFingerprint")
    private final String schemaFingerprint;

    @JsonProperty("lookupFieldId")
    private final int lookupFieldId;

    @JsonProperty("valueFieldIds")
    private final int[] valueFieldIds;

    @JsonProperty("servedGeneration")
    private final long servedGeneration;

    @JsonProperty("servedSnapshotId")
    private final long servedSnapshotId;

    @JsonProperty("snapshotUuid")
    @Nullable
    private final String snapshotUuid;

    @JsonProperty("hashVersion")
    private final int hashVersion;

    @JsonProperty("layout")
    private final String layout;

    @JsonProperty("ownerToken")
    private final String ownerToken;

    @JsonProperty("ready")
    private final boolean ready;

    @JsonProperty("reason")
    private final String reason;

    @JsonProperty("endpoints")
    private final Endpoint[] endpoints;

    @JsonCreator
    public GlobalIndexQueryServiceDescriptor(
            @JsonProperty("protocolVersion") int protocolVersion,
            @JsonProperty("tableUuid") String tableUuid,
            @JsonProperty("branch") String branch,
            @JsonProperty("schemaId") long schemaId,
            @JsonProperty("schemaFingerprint") String schemaFingerprint,
            @JsonProperty("lookupFieldId") int lookupFieldId,
            @JsonProperty("valueFieldIds") int[] valueFieldIds,
            @JsonProperty("servedGeneration") long servedGeneration,
            @JsonProperty("servedSnapshotId") long servedSnapshotId,
            @JsonProperty("snapshotUuid") @Nullable String snapshotUuid,
            @JsonProperty("hashVersion") int hashVersion,
            @JsonProperty("layout") String layout,
            @JsonProperty("ownerToken") String ownerToken,
            @JsonProperty("ready") boolean ready,
            @JsonProperty("reason") String reason,
            @JsonProperty("endpoints") Endpoint[] endpoints) {
        this.protocolVersion = protocolVersion;
        this.tableUuid = tableUuid;
        this.branch = branch;
        this.schemaId = schemaId;
        this.schemaFingerprint = schemaFingerprint;
        this.lookupFieldId = lookupFieldId;
        this.valueFieldIds = Arrays.copyOf(valueFieldIds, valueFieldIds.length);
        this.servedGeneration = servedGeneration;
        this.servedSnapshotId = servedSnapshotId;
        this.snapshotUuid = snapshotUuid;
        this.hashVersion = hashVersion;
        this.layout = layout;
        this.ownerToken = ownerToken;
        this.ready = ready;
        this.reason = reason;
        this.endpoints = Arrays.copyOf(endpoints, endpoints.length);
    }

    public int protocolVersion() {
        return protocolVersion;
    }

    public String tableUuid() {
        return tableUuid;
    }

    public String branch() {
        return branch;
    }

    public long schemaId() {
        return schemaId;
    }

    public String schemaFingerprint() {
        return schemaFingerprint;
    }

    public int lookupFieldId() {
        return lookupFieldId;
    }

    public int[] valueFieldIds() {
        return Arrays.copyOf(valueFieldIds, valueFieldIds.length);
    }

    public long servedGeneration() {
        return servedGeneration;
    }

    public long servedSnapshotId() {
        return servedSnapshotId;
    }

    @Nullable
    public String snapshotUuid() {
        return snapshotUuid;
    }

    public int hashVersion() {
        return hashVersion;
    }

    public String layout() {
        return layout;
    }

    public String ownerToken() {
        return ownerToken;
    }

    public boolean ready() {
        return ready;
    }

    public String reason() {
        return reason;
    }

    public Endpoint[] endpoints() {
        return Arrays.copyOf(endpoints, endpoints.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GlobalIndexQueryServiceDescriptor)) {
            return false;
        }
        GlobalIndexQueryServiceDescriptor that = (GlobalIndexQueryServiceDescriptor) o;
        return protocolVersion == that.protocolVersion
                && schemaId == that.schemaId
                && lookupFieldId == that.lookupFieldId
                && servedGeneration == that.servedGeneration
                && servedSnapshotId == that.servedSnapshotId
                && hashVersion == that.hashVersion
                && ready == that.ready
                && Objects.equals(tableUuid, that.tableUuid)
                && Objects.equals(branch, that.branch)
                && schemaFingerprint.equals(that.schemaFingerprint)
                && Arrays.equals(valueFieldIds, that.valueFieldIds)
                && Objects.equals(snapshotUuid, that.snapshotUuid)
                && layout.equals(that.layout)
                && ownerToken.equals(that.ownerToken)
                && reason.equals(that.reason)
                && Arrays.equals(endpoints, that.endpoints);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        protocolVersion,
                        tableUuid,
                        branch,
                        schemaId,
                        schemaFingerprint,
                        lookupFieldId,
                        servedGeneration,
                        servedSnapshotId,
                        snapshotUuid,
                        hashVersion,
                        layout,
                        ownerToken,
                        ready,
                        reason);
        result = 31 * result + Arrays.hashCode(valueFieldIds);
        result = 31 * result + Arrays.hashCode(endpoints);
        return result;
    }

    /** One independently fenced query shard published by a service executor. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Endpoint implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("shardId")
        private final int shardId;

        @JsonProperty("address")
        private final InetSocketAddress address;

        @JsonProperty("serverEpoch")
        private final String serverEpoch;

        @JsonCreator
        public Endpoint(
                @JsonProperty("shardId") int shardId,
                @JsonProperty("address") InetSocketAddress address,
                @JsonProperty("serverEpoch") String serverEpoch) {
            this.shardId = shardId;
            this.address = address;
            this.serverEpoch = serverEpoch;
        }

        public int shardId() {
            return shardId;
        }

        public InetSocketAddress address() {
            return address;
        }

        public String serverEpoch() {
            return serverEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Endpoint)) {
                return false;
            }
            Endpoint endpoint = (Endpoint) o;
            return shardId == endpoint.shardId
                    && address.equals(endpoint.address)
                    && serverEpoch.equals(endpoint.serverEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, address, serverEpoch);
        }
    }
}
