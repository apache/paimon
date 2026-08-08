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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.Endpoint;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.QueryServiceNotReadyException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION;
import static org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.LAYOUT;
import static org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION;
import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID;
import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.route;

/** Service-manager backed {@link GlobalIndexQueryLocation}. */
public class GlobalIndexQueryLocationImpl implements GlobalIndexQueryLocation {

    private final ServiceManager manager;
    private final String serviceId;
    private final String expectedTableUuid;
    private final String expectedBranch;
    private final long expectedSchemaId;
    private final String expectedSchemaFingerprint;

    private volatile GlobalIndexQueryServiceDescriptor descriptorCache;

    public GlobalIndexQueryLocationImpl(
            ServiceManager manager,
            String expectedTableUuid,
            String expectedBranch,
            long expectedSchemaId,
            QuerySpec spec) {
        this.manager = manager;
        this.serviceId = spec.serviceId();
        this.expectedTableUuid = expectedTableUuid;
        this.expectedBranch = expectedBranch;
        this.expectedSchemaId = expectedSchemaId;
        this.expectedSchemaFingerprint = spec.schemaFingerprint();
    }

    @Override
    public GlobalIndexQueryEndpoint getLocation(BinaryRow key, boolean forceUpdate)
            throws IOException {
        GlobalIndexQueryServiceDescriptor descriptor = descriptorCache;
        if (descriptor == null || forceUpdate) {
            descriptor = loadDescriptor();
            descriptorCache = descriptor;
        }
        // Keep one immutable descriptor snapshot for the whole endpoint. A concurrent forced
        // refresh must not combine an old address/epoch with a new generation fence.
        Endpoint[] endpoints = descriptor.endpoints();
        int shardId = route(key, endpoints.length);
        Endpoint endpoint = endpoints[shardId];
        return new GlobalIndexQueryEndpoint(
                shardId,
                endpoint.address(),
                endpoint.serverEpoch(),
                descriptor.servedGeneration(),
                descriptor.servedSnapshotId(),
                descriptor.snapshotUuid());
    }

    /**
     * Returns whether discovery currently contains a ready descriptor valid for this table/spec.
     */
    public boolean isServiceReady() {
        try {
            GlobalIndexQueryServiceDescriptor descriptor = loadDescriptor();
            descriptorCache = descriptor;
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private GlobalIndexQueryServiceDescriptor loadDescriptor() throws IOException {
        final Optional<GlobalIndexQueryServiceDescriptor> descriptor;
        try {
            descriptor = manager.globalIndexService(serviceId);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (RuntimeException e) {
            throw new IOException(
                    "Cannot decode global-index query service '"
                            + serviceId
                            + "' for table path: "
                            + manager.tablePath(),
                    e);
        }
        if (!descriptor.isPresent()) {
            throw new QueryServiceNotReadyException(
                    "Cannot find global-index query service '"
                            + serviceId
                            + "' for table path: "
                            + manager.tablePath());
        }

        GlobalIndexQueryServiceDescriptor result = descriptor.get();
        if (!result.ready()) {
            throw new QueryServiceNotReadyException(
                    "Global-index query service '"
                            + serviceId
                            + "' is not ready: "
                            + result.reason());
        }
        validateDescriptor(result);
        return result;
    }

    private void validateDescriptor(GlobalIndexQueryServiceDescriptor descriptor)
            throws IOException {
        if (descriptor.protocolVersion() != PROTOCOL_VERSION
                || descriptor.hashVersion() != KEY_HASH_VERSION
                || !LAYOUT.equals(descriptor.layout())) {
            throw new IOException(
                    "Unsupported global-index query service descriptor for '"
                            + serviceId
                            + "': protocol="
                            + descriptor.protocolVersion()
                            + ", hash="
                            + descriptor.hashVersion()
                            + ", layout="
                            + descriptor.layout());
        }
        if (!expectedTableUuid.equals(descriptor.tableUuid())
                || !expectedBranch.equals(descriptor.branch())) {
            throw new IOException(
                    "Global-index query service table identity does not match the requested table."
                            + " Expected UUID "
                            + expectedTableUuid
                            + " on branch "
                            + expectedBranch
                            + ", but found UUID "
                            + descriptor.tableUuid()
                            + " on branch "
                            + descriptor.branch()
                            + '.');
        }
        if (descriptor.schemaId() != expectedSchemaId
                || !expectedSchemaFingerprint.equals(descriptor.schemaFingerprint())) {
            throw new IOException(
                    "Global-index query service schema does not match the requested table schema."
                            + " Expected schema "
                            + expectedSchemaId
                            + " with fingerprint "
                            + expectedSchemaFingerprint
                            + ", but found "
                            + descriptor.schemaId()
                            + " with fingerprint "
                            + descriptor.schemaFingerprint()
                            + '.');
        }
        if (descriptor.servedGeneration() < EMPTY_SNAPSHOT_ID
                || descriptor.servedSnapshotId() < EMPTY_SNAPSHOT_ID
                || descriptor.snapshotUuid() != null && descriptor.snapshotUuid().isEmpty()) {
            throw new IOException(
                    "Global-index query service '"
                            + serviceId
                            + "' has an invalid generation/snapshot fence.");
        }
        Endpoint[] endpoints = descriptor.endpoints();
        if (endpoints.length == 0) {
            throw new IOException(
                    "Global-index query service '" + serviceId + "' has an invalid endpoint set.");
        }
        for (int i = 0; i < endpoints.length; i++) {
            Endpoint endpoint = endpoints[i];
            if (endpoint == null
                    || endpoint.shardId() != i
                    || endpoint.address() == null
                    || endpoint.address().getPort() <= 0
                    || endpoint.serverEpoch() == null
                    || endpoint.serverEpoch().isEmpty()) {
                throw new IOException(
                        "Global-index query service '"
                                + serviceId
                                + "' has an invalid endpoint at shard "
                                + i
                                + '.');
            }
        }
    }
}
