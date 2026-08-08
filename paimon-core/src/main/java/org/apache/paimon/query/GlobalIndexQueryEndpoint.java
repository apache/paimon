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

package org.apache.paimon.query;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

/** A snapshot-fenced endpoint for one logical global-index key shard. */
public class GlobalIndexQueryEndpoint implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int shardId;
    private final InetSocketAddress address;
    private final String serverEpoch;
    private final long servedGeneration;
    private final long servedSnapshotId;
    @Nullable private final String snapshotUuid;

    public GlobalIndexQueryEndpoint(
            int shardId,
            InetSocketAddress address,
            String serverEpoch,
            long servedGeneration,
            long servedSnapshotId,
            @Nullable String snapshotUuid) {
        this.shardId = shardId;
        this.address = address;
        this.serverEpoch = serverEpoch;
        this.servedGeneration = servedGeneration;
        this.servedSnapshotId = servedSnapshotId;
        this.snapshotUuid = snapshotUuid;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GlobalIndexQueryEndpoint)) {
            return false;
        }
        GlobalIndexQueryEndpoint that = (GlobalIndexQueryEndpoint) o;
        return shardId == that.shardId
                && servedGeneration == that.servedGeneration
                && servedSnapshotId == that.servedSnapshotId
                && address.equals(that.address)
                && serverEpoch.equals(that.serverEpoch)
                && Objects.equals(snapshotUuid, that.snapshotUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                shardId, address, serverEpoch, servedGeneration, servedSnapshotId, snapshotUuid);
    }
}
