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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.utils.SinkContextUtils;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.Endpoint;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/** Publishes a generation only after every global-index executor reports ready. */
public class GlobalIndexQueryAddressRegister implements Sink<InternalRow> {

    private final ServiceManager serviceManager;
    private final String serviceId;
    private final String tableUuid;
    private final String branch;
    private final long schemaId;
    private final String schemaFingerprint;
    private final int lookupFieldId;
    private final int[] valueFieldIds;
    private final String publisherId;

    public GlobalIndexQueryAddressRegister(FileStoreTable table, QuerySpec spec) {
        this.serviceManager = table.store().newServiceManager();
        this.serviceId = spec.serviceId();
        this.tableUuid = table.uuid();
        this.branch = table.coreOptions().branch();
        this.schemaId = table.schema().id();
        this.schemaFingerprint = spec.schemaFingerprint();
        this.lookupFieldId = spec.lookupFieldId();
        this.valueFieldIds = spec.valueFieldIds();
        this.publisherId = UUID.randomUUID().toString();
    }

    /** Do not annotate to maintain compatibility with Flink 2.0+. */
    public SinkWriter<InternalRow> createWriter(InitContext context) {
        return new Writer(
                serviceManager,
                serviceId,
                tableUuid,
                branch,
                schemaId,
                schemaFingerprint,
                lookupFieldId,
                valueFieldIds,
                nextOwnerToken(SinkContextUtils.getAttemptNumber(context)));
    }

    /** Do not annotate to maintain compatibility with Flink 1.18-. */
    public SinkWriter<InternalRow> createWriter(WriterInitContext context) {
        return new Writer(
                serviceManager,
                serviceId,
                tableUuid,
                branch,
                schemaId,
                schemaFingerprint,
                lookupFieldId,
                valueFieldIds,
                nextOwnerToken(SinkContextUtils.getAttemptNumber(context)));
    }

    private String nextOwnerToken(int attemptNumber) {
        long sequence = serviceManager.nextGlobalIndexOwnerSequence(serviceId);
        return String.format(Locale.ROOT, "%019d-%010d-%s", sequence, attemptNumber, publisherId);
    }

    private static class Writer implements SinkWriter<InternalRow> {

        private final ServiceManager serviceManager;
        private final String serviceId;
        private final String tableUuid;
        private final String branch;
        private final long schemaId;
        private final String schemaFingerprint;
        private final int lookupFieldId;
        private final int[] valueFieldIds;
        private final String ownerToken;
        private final Map<Long, Candidate> candidates = new HashMap<>();

        private long eventGeneration = Long.MIN_VALUE;
        private long publishedServedGeneration = Long.MIN_VALUE;
        private int numExecutors;
        private UnavailableCandidate unavailableCandidate;

        private Writer(
                ServiceManager serviceManager,
                String serviceId,
                String tableUuid,
                String branch,
                long schemaId,
                String schemaFingerprint,
                int lookupFieldId,
                int[] valueFieldIds,
                String ownerToken) {
            this.serviceManager = serviceManager;
            this.serviceId = serviceId;
            this.tableUuid = tableUuid;
            this.branch = branch;
            this.schemaId = schemaId;
            this.schemaFingerprint = schemaFingerprint;
            this.lookupFieldId = lookupFieldId;
            this.valueFieldIds = valueFieldIds;
            this.ownerToken = ownerToken;
            publishNotReady(
                    "Query service attempt is bootstrapping.",
                    Long.MIN_VALUE,
                    GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID);
        }

        @Override
        public void write(InternalRow row, Context context) {
            long rowEventGeneration = row.getLong(0);
            if (rowEventGeneration < eventGeneration) {
                return;
            }
            int eventNumExecutors = row.getInt(2);
            if (rowEventGeneration > eventGeneration) {
                eventGeneration = rowEventGeneration;
                numExecutors = eventNumExecutors;
                candidates.clear();
                unavailableCandidate = null;
                // Withdraw discovery as soon as the first executor observes the generation. The
                // empty snapshot fence deliberately does not acknowledge the target yet; the
                // monitor starts its retention grace only after every executor reports that its
                // accepted-generation fence has advanced.
                publishNotReady(
                        "Refreshing global-index query snapshot.",
                        rowEventGeneration,
                        GlobalIndexQueryServiceUtils.EMPTY_SNAPSHOT_ID);
            } else if (numExecutors != eventNumExecutors) {
                throw new IllegalArgumentException(
                        "Executor count changed within global-index event generation "
                                + eventGeneration);
            }

            if (!row.getBoolean(1)) {
                int executorId = row.getInt(3);
                if (executorId < 0 || executorId >= numExecutors) {
                    throw new IllegalArgumentException(
                            "Invalid global-index executor ID "
                                    + executorId
                                    + " for "
                                    + numExecutors
                                    + " executors.");
                }
                long targetSnapshotId = row.getLong(8);
                String reason = row.getString(6).toString();
                if (unavailableCandidate == null) {
                    unavailableCandidate = new UnavailableCandidate(targetSnapshotId);
                } else {
                    unavailableCandidate.checkSnapshot(targetSnapshotId);
                }
                unavailableCandidate.acknowledge(executorId, reason);
                if (unavailableCandidate.size() == numExecutors) {
                    publishNotReady(
                            unavailableCandidate.reason(), rowEventGeneration, targetSnapshotId);
                }
                return;
            }

            long servedGeneration = row.getLong(7);
            if (servedGeneration != rowEventGeneration) {
                // An executor may still serve its previous shadow generation locally while a
                // rebuild is in progress. It must not republish that old generation after the
                // discovery descriptor has been withdrawn.
                return;
            }
            if (servedGeneration < publishedServedGeneration) {
                return;
            }
            long servedSnapshotId = row.getLong(8);
            String snapshotUuid = row.isNullAt(9) ? null : row.getString(9).toString();
            Candidate candidate =
                    candidates.computeIfAbsent(
                            servedGeneration,
                            ignored -> new Candidate(servedSnapshotId, snapshotUuid));
            candidate.checkSnapshot(servedSnapshotId, snapshotUuid);
            candidate.put(
                    row.getInt(3),
                    new InetSocketAddress(row.getString(4).toString(), row.getInt(5)),
                    row.getString(10).toString());
            if (candidate.size() == numExecutors) {
                serviceManager.resetGlobalIndexService(
                        serviceId,
                        descriptor(
                                true,
                                "",
                                servedGeneration,
                                servedSnapshotId,
                                snapshotUuid,
                                candidate.endpoints(numExecutors)));
                publishedServedGeneration = servedGeneration;
                candidates.entrySet().removeIf(entry -> entry.getKey() < servedGeneration);
            }
        }

        private void publishNotReady(String reason, long generation, long snapshotId) {
            serviceManager.resetGlobalIndexService(
                    serviceId,
                    descriptor(false, reason, generation, snapshotId, null, new Endpoint[0]));
        }

        private GlobalIndexQueryServiceDescriptor descriptor(
                boolean ready,
                String reason,
                long servedGeneration,
                long servedSnapshotId,
                String snapshotUuid,
                Endpoint[] endpoints) {
            return new GlobalIndexQueryServiceDescriptor(
                    GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                    tableUuid,
                    branch,
                    schemaId,
                    schemaFingerprint,
                    lookupFieldId,
                    valueFieldIds,
                    servedGeneration,
                    servedSnapshotId,
                    snapshotUuid,
                    GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                    GlobalIndexQueryServiceDescriptor.LAYOUT,
                    ownerToken,
                    ready,
                    reason,
                    endpoints);
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {
            serviceManager.deleteGlobalIndexServiceIfOwned(serviceId, ownerToken);
        }

        private static class Candidate {

            private final long snapshotId;
            private final String snapshotUuid;
            private final TreeMap<Integer, InetSocketAddress> addresses = new TreeMap<>();
            private final TreeMap<Integer, String> serverEpochs = new TreeMap<>();

            private Candidate(long snapshotId, String snapshotUuid) {
                this.snapshotId = snapshotId;
                this.snapshotUuid = snapshotUuid;
            }

            private void checkSnapshot(long snapshotId, String snapshotUuid) {
                if (this.snapshotId != snapshotId
                        || !java.util.Objects.equals(this.snapshotUuid, snapshotUuid)) {
                    throw new IllegalArgumentException(
                            "Executors reported different snapshots for one served generation.");
                }
            }

            private void put(int executorId, InetSocketAddress address, String serverEpoch) {
                addresses.put(executorId, address);
                serverEpochs.put(executorId, serverEpoch);
            }

            private int size() {
                return addresses.size();
            }

            private Endpoint[] endpoints(int expectedSize) {
                checkExecutorIds(expectedSize);
                Endpoint[] result = new Endpoint[expectedSize];
                for (int shard = 0; shard < expectedSize; shard++) {
                    result[shard] =
                            new Endpoint(shard, addresses.get(shard), serverEpochs.get(shard));
                }
                return result;
            }

            private void checkExecutorIds(int expectedSize) {
                for (int i = 0; i < expectedSize; i++) {
                    if (!addresses.containsKey(i) || !serverEpochs.containsKey(i)) {
                        throw new IllegalArgumentException("Missing executor ID " + i);
                    }
                }
            }
        }

        private static class UnavailableCandidate {

            private final long snapshotId;
            private final Map<Integer, String> acknowledgements = new HashMap<>();
            private String latestReason;

            private UnavailableCandidate(long snapshotId) {
                this.snapshotId = snapshotId;
            }

            private void checkSnapshot(long snapshotId) {
                if (this.snapshotId != snapshotId) {
                    throw new IllegalArgumentException(
                            "Executors reported different unavailable global-index snapshots for"
                                    + " one generation: "
                                    + this.snapshotId
                                    + " and "
                                    + snapshotId);
                }
            }

            private void acknowledge(int executorId, String reason) {
                acknowledgements.put(executorId, reason);
                if (reason != null
                        && !reason.isEmpty()
                        && (latestReason == null
                                || isRefreshing(latestReason)
                                || !isRefreshing(reason))) {
                    latestReason = reason;
                }
            }

            private int size() {
                return acknowledgements.size();
            }

            private String reason() {
                return latestReason == null
                        ? "Global-index query snapshot is unavailable."
                        : latestReason;
            }

            private static boolean isRefreshing(String reason) {
                return "Refreshing".equalsIgnoreCase(reason);
            }
        }
    }
}
