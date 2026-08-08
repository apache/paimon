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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.procedure.QueryServiceProcedure;
import org.apache.paimon.flink.query.RemoteGlobalIndexTableQuery;
import org.apache.paimon.flink.service.QueryService;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.query.GlobalIndexQueryEndpoint;
import org.apache.paimon.query.GlobalIndexQueryLocation;
import org.apache.paimon.query.GlobalIndexQueryLocationImpl;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.Endpoint;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.client.GlobalIndexQueryClient;
import org.apache.paimon.service.client.GlobalIndexQueryClient.LookupResult;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.QueryServiceNotReadyException;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.STALE_GENERATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** End-to-end MiniCluster tests for the data-evolution global-index query service. */
@SuppressWarnings("BusyWait")
public class GlobalIndexQueryServiceITCase extends CatalogITCaseBase {

    private static final String TABLE = "IMAGE_BLOB";
    private static final String LOOKUP_FIELD = "url";
    private static final String VALUE_FIELD = "descriptor";
    private static final String CONSUMER_ID = "global-index-query-it";
    private static final Duration LEASE_GRACE_PERIOD = Duration.ofSeconds(2);
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final InternalRowSerializer KEY_SERIALIZER =
            InternalSerializers.create(RowType.of(DataTypes.STRING()));

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE "
                        + TABLE
                        + " (url STRING, descriptor BYTES) WITH ("
                        + "'bucket'='-1', "
                        + "'global-index.enabled'='true', "
                        + "'row-tracking.enabled'='true', "
                        + "'data-evolution.enabled'='true', "
                        + "'blob-field'='descriptor', "
                        + "'blob-compaction.enabled'='true', "
                        + "'compaction.min.file-num'='2', "
                        + "'continuous.discovery-interval'='20 ms', "
                        + "'consumer.expiration-time'='1 h')");
    }

    @Test
    @Timeout(300)
    public void testSnapshotFencedBlobLifecycleAndRescale() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        LinkedHashMap<String, byte[]> expected = initialRowsCoveringBothShards();
        for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
            insert(entry.getKey(), entry.getValue());
        }
        buildBTreeIndex();

        FileStoreTable table = paimonTable(TABLE);
        QuerySpec spec = querySpec(table);
        long initialSnapshotId = table.snapshotManager().latestSnapshot().id();
        long initialBlobFiles = blobFileCount();
        assertThat(initialBlobFiles).isGreaterThan(1L);

        GlobalIndexQueryServiceDescriptor compactedDescriptor;
        JobClient initialJob = startQueryService(table, 2);
        try {
            GlobalIndexQueryServiceDescriptor initialDescriptor =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.servedSnapshotId() == initialSnapshotId
                                            && descriptor.endpoints().length == 2,
                            initialJob,
                            "initial two-shard service");
            assertThat(initialDescriptor.tableUuid()).isEqualTo(table.uuid());
            assertThat(initialDescriptor.branch()).isEqualTo(table.coreOptions().branch());
            assertThat(initialDescriptor.snapshotUuid()).isNotBlank();
            assertBatchLookup(table, spec, expected, true);

            String existingKey = expected.keySet().iterator().next();

            String appendedKey = "O1CN-appended-after-service.jpg";
            byte[] appendedValue = "image-appended".getBytes(StandardCharsets.UTF_8);
            // Prime and retain a client so its location cache contains the old ready descriptor.
            // Once the executor observes the append generation, the same cached endpoint must
            // fail closed for the new key instead of answering null from its retained old state.
            try (ClientResource cachedClient = client(table, spec)) {
                BinaryRow[] beforeAppend =
                        cachedClient
                                .client
                                .getValues(new BinaryRow[] {key(existingKey)})
                                .get(30, TimeUnit.SECONDS);
                assertThat(beforeAppend[0]).isNotNull();

                insert(appendedKey, appendedValue);
                expected.put(appendedKey, appendedValue);
                long uncoveredSnapshotId = table.snapshotManager().latestSnapshot().id();
                assertThat(uncoveredSnapshotId).isGreaterThan(initialDescriptor.servedSnapshotId());

                GlobalIndexQueryServiceDescriptor unavailable =
                        waitForDescriptor(
                                table,
                                spec,
                                descriptor ->
                                        !descriptor.ready()
                                                && descriptor
                                                        .reason()
                                                        .toLowerCase()
                                                        .contains("btree"),
                                initialJob,
                                "uncovered snapshot to withdraw discovery");
                assertThat(unavailable.reason()).containsIgnoringCase("BTree");
                Throwable cachedFailure =
                        failure(cachedClient.client.getValues(new BinaryRow[] {key(appendedKey)}));
                assertThat(cachedFailure).isInstanceOf(QueryServiceNotReadyException.class);
                assertFreshClientNotReady(table, spec, key(appendedKey));

                String secondUnindexedKey = "O1CN-second-unindexed-tail.jpg";
                byte[] secondUnindexedValue =
                        "image-second-unindexed".getBytes(StandardCharsets.UTF_8);
                insert(secondUnindexedKey, secondUnindexedValue);
                expected.put(secondUnindexedKey, secondUnindexedValue);
                long secondUnindexedSnapshotId = table.snapshotManager().latestSnapshot().id();
                assertThat(secondUnindexedSnapshotId).isGreaterThan(uncoveredSnapshotId);
                waitForConsumerSnapshot(
                        table,
                        secondUnindexedSnapshotId,
                        initialJob,
                        "continuous NOT_READY lease advancement");
                expireAllButLatest(table);
                assertThat(table.snapshotManager().earliestSnapshotId())
                        .isEqualTo(secondUnindexedSnapshotId);
                assertThat(
                                table.fileIO()
                                        .exists(
                                                table.snapshotManager()
                                                        .snapshotPath(initialSnapshotId)))
                        .isFalse();
                Throwable secondCachedFailure =
                        failure(
                                cachedClient.client.getValues(
                                        new BinaryRow[] {key(secondUnindexedKey)}));
                assertThat(secondCachedFailure).isInstanceOf(QueryServiceNotReadyException.class);
            }

            buildBTreeIndex();
            long refreshedSnapshotId = table.snapshotManager().latestSnapshot().id();
            GlobalIndexQueryServiceDescriptor refreshed =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.servedSnapshotId() == refreshedSnapshotId
                                            && descriptor.servedGeneration()
                                                    > initialDescriptor.servedGeneration(),
                            initialJob,
                            "newly indexed generation");
            assertThat(refreshed.endpoints()).hasSize(2);
            assertBatchLookup(table, spec, expected, true);

            BlobDescriptor descriptorBeforeCompaction = lookupDescriptor(table, spec, existingKey);
            GlobalIndexQueryLocation oldFence = frozenLocation(refreshed);
            long blobsBeforeCompaction = blobFileCount();
            batchSql("CALL sys.compact(`table` => 'default.%s')", TABLE);
            long compactSnapshotId = table.snapshotManager().latestSnapshot().id();
            assertThat(compactSnapshotId).isGreaterThan(refreshed.servedSnapshotId());
            assertThat(blobFileCount()).isLessThan(blobsBeforeCompaction);

            // The exact compacted target has withdrawn the old descriptor, but the non-zero
            // handover grace still protects a BlobDescriptor returned by the previous generation.
            // Snapshot expiry in this window must not delete its referenced Blob file.
            waitForDescriptor(
                    table,
                    spec,
                    descriptor -> descriptor.servedSnapshotId() == compactSnapshotId,
                    initialJob,
                    "compaction handover acknowledgement");
            expireAllButLatest(table);
            assertBlobReadable(table, descriptorBeforeCompaction, expected.get(existingKey));

            // Refreshing an existing BTree is idempotent when compaction preserved its exact
            // coverage, and repairs coverage when compaction replaced a covered row range.
            buildBTreeIndex();
            long postCompactionSnapshotId = table.snapshotManager().latestSnapshot().id();
            compactedDescriptor =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.servedSnapshotId()
                                                    == postCompactionSnapshotId
                                            && descriptor.servedGeneration()
                                                    > refreshed.servedGeneration(),
                            initialJob,
                            "post-compaction generation");
            BlobDescriptor descriptorAfterCompaction = lookupDescriptor(table, spec, existingKey);
            assertThat(descriptorAfterCompaction.uri())
                    .isNotEqualTo(descriptorBeforeCompaction.uri());
            assertBlobReadable(table, descriptorAfterCompaction, expected.get(existingKey));
            assertStaleFenceRejected(oldFence, key(existingKey));
            assertBatchLookup(table, spec, expected, true);

            // Once the exact replacement generation has been acknowledged for the full grace,
            // its lease may advance and ordinary expiry can reclaim the old Blob file.
            waitForConsumerSnapshot(
                    table,
                    postCompactionSnapshotId,
                    initialJob,
                    "post-compaction Blob handover grace");
            expireAllButLatest(table);
            assertThat(table.fileIO().exists(new Path(descriptorBeforeCompaction.uri()))).isFalse();
        } finally {
            cancel(initialJob);
        }

        GlobalIndexQueryServiceDescriptor cancelled =
                waitForDescriptor(
                        table,
                        spec,
                        descriptor -> !descriptor.ready(),
                        null,
                        "cancelled service tombstone");
        assertThat(cancelled.reason()).containsIgnoringCase("closed");
        assertFreshClientNotReady(table, spec, key(expected.keySet().iterator().next()));

        List<String> stoppedAttemptLeases =
                table.consumerManager().listAllIds().stream()
                        .filter(id -> id.startsWith(CONSUMER_ID + '-'))
                        .collect(Collectors.toList());
        assertThat(stoppedAttemptLeases).isNotEmpty();

        // Source close is also used for Flink failover. The stopped attempt lease must protect the
        // last served descriptor until a replacement attempt pins its snapshot, even if snapshot
        // expiration runs in the handover gap.
        String recoveryKey = "O1CN-recovery-gap.jpg";
        byte[] recoveryValue = "image-recovery-gap".getBytes(StandardCharsets.UTF_8);
        insert(recoveryKey, recoveryValue);
        expected.put(recoveryKey, recoveryValue);
        expireAllButLatest(table);
        assertThat(
                        table.fileIO()
                                .exists(
                                        table.snapshotManager()
                                                .snapshotPath(
                                                        compactedDescriptor.servedSnapshotId())))
                .isTrue();
        buildBTreeIndex();
        long recoverySnapshotId = table.snapshotManager().latestSnapshot().id();

        GlobalIndexQueryServiceDescriptor singleShardDescriptor;
        JobClient oneShardJob = startQueryService(table, 1);
        try {
            singleShardDescriptor =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.servedGeneration()
                                                    > compactedDescriptor.servedGeneration()
                                            && descriptor.servedSnapshotId() == recoverySnapshotId
                                            && descriptor.endpoints().length == 1,
                            oneShardJob,
                            "single-shard restart");
            assertThat(singleShardDescriptor.ownerToken())
                    .isNotEqualTo(compactedDescriptor.ownerToken());
            assertThat(singleShardDescriptor.endpoints()[0].serverEpoch())
                    .isNotEqualTo(compactedDescriptor.endpoints()[0].serverEpoch());
            assertBatchLookup(table, spec, expected, false);
        } finally {
            cancel(oneShardJob);
        }
        waitForDescriptor(
                table,
                spec,
                descriptor -> !descriptor.ready(),
                null,
                "single-shard cancellation tombstone");

        Set<String> leasesBeforeRescale = new HashSet<>(table.consumerManager().listAllIds());
        JobClient rescaledJob = startQueryService(table, 2);
        try {
            GlobalIndexQueryServiceDescriptor rescaled =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.servedGeneration()
                                                    == singleShardDescriptor.servedGeneration()
                                            && descriptor.endpoints().length == 2,
                            rescaledJob,
                            "P1 to P2 rebuild");
            assertThat(rescaled.ownerToken()).isNotEqualTo(singleShardDescriptor.ownerToken());
            List<String> rescaledLeaseIds =
                    table.consumerManager().listAllIds().stream()
                            .filter(id -> id.startsWith(CONSUMER_ID + '-'))
                            .filter(id -> !leasesBeforeRescale.contains(id))
                            .collect(Collectors.toList());
            // Each restarted source attempt owns a distinct UUID lease. Stopped attempts are
            // deliberately retained until consumer expiration, so a healthy rescale may create
            // more than one lease even though only one replacement attempt remains live.
            assertThat(rescaledLeaseIds).isNotEmpty();
            assertThat(
                            expected.keySet().stream()
                                    .map(GlobalIndexQueryServiceITCase::key)
                                    .map(
                                            binaryKey ->
                                                    GlobalIndexQueryServiceUtils.route(
                                                            binaryKey, 2))
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(0, 1);
            assertBatchLookup(table, spec, expected, true);

            // Exact BTree coverage is only the bootstrap gate. A duplicate discovered while
            // materializing a covered snapshot must publish exact NOT_READY and, after the
            // non-zero grace, advance this attempt's lease instead of pinning history forever.
            String duplicateKey = expected.keySet().iterator().next();
            insert(duplicateKey, "duplicate-image".getBytes(StandardCharsets.UTF_8));
            buildBTreeIndex();
            long duplicateSnapshotId = table.snapshotManager().latestSnapshot().id();
            waitForDescriptor(
                    table,
                    spec,
                    descriptor ->
                            !descriptor.ready()
                                    && descriptor.servedSnapshotId() == duplicateSnapshotId
                                    && descriptor.reason().toLowerCase().contains("duplicate"),
                    rescaledJob,
                    "covered duplicate bootstrap rejection");
            waitForAnyNewConsumerSnapshot(
                    table,
                    leasesBeforeRescale,
                    duplicateSnapshotId,
                    rescaledJob,
                    "bootstrap-invalid lease advancement");
        } finally {
            cancel(rescaledJob);
        }
    }

    @Test
    @Timeout(180)
    public void testPublicActionProcedureAndRemoteTableQuery() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        String lookupKey = "O1CN-public-entrypoint.jpg";
        byte[] expected = "public-entrypoint-image".getBytes(StandardCharsets.UTF_8);
        insert(lookupKey, expected);
        buildBTreeIndex();

        FileStoreTable table = paimonTable(TABLE);
        QuerySpec spec = querySpec(table);
        String actionJobName = "global-index-query-service-action-it";
        Action action =
                ActionFactory.createAction(
                                new String[] {
                                    "query_service",
                                    "--warehouse",
                                    path,
                                    "--database",
                                    "default",
                                    "--table",
                                    TABLE,
                                    "--parallelism",
                                    "2",
                                    "--lookup-key",
                                    LOOKUP_FIELD,
                                    "--value-fields",
                                    VALUE_FIELD,
                                    "--consumer-id",
                                    "global-index-action-it",
                                    "--lease-grace-period",
                                    "2 s"
                                })
                        .orElseThrow(
                                () -> new AssertionError("Query-service action was not found"));
        assertThat(action).isInstanceOf(ActionBase.class);
        StreamExecutionEnvironment actionEnv =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .allowRestart()
                        .build();
        Configuration actionConfiguration = new Configuration();
        actionConfiguration.set(PipelineOptions.NAME, actionJobName);
        actionEnv.configure(actionConfiguration);
        ((ActionBase) action).withStreamExecutionEnvironment(actionEnv);

        CompletableFuture<Void> actionExecution = runActionAsync(action);
        GlobalIndexQueryServiceDescriptor actionDescriptor =
                waitForActionDescriptor(
                        table,
                        spec,
                        descriptor -> descriptor.ready() && descriptor.endpoints().length == 2,
                        actionExecution,
                        "ActionFactory global-index service");
        assertThat(actionDescriptor.ownerToken()).isNotBlank();
        assertThat(table.consumerManager().listAllIds())
                .anyMatch(id -> id.startsWith("global-index-action-it-"));
        assertRemoteTableQuery(table, lookupKey, expected);

        cancelClusterJob(actionJobName);
        awaitActionTermination(actionExecution);
        waitForDescriptor(
                table,
                spec,
                descriptor ->
                        !descriptor.ready()
                                && descriptor.ownerToken().equals(actionDescriptor.ownerToken()),
                null,
                "ActionFactory cancellation tombstone");

        GlobalIndexQueryServiceDescriptor procedureDescriptor;
        try (CloseableIterator<Row> procedure =
                streamSqlIter(
                        "CALL sys.query_service("
                                + "`table` => 'default.%s', parallelism => 2, "
                                + "lookup_key => '%s', value_fields => '%s', "
                                + "consumer_id => 'global-index-procedure-it', "
                                + "lease_grace_period => '2 s')",
                        TABLE, LOOKUP_FIELD, VALUE_FIELD)) {
            procedureDescriptor =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.endpoints().length == 2
                                            && !descriptor
                                                    .ownerToken()
                                                    .equals(actionDescriptor.ownerToken()),
                            null,
                            "named query-service procedure");
            assertThat(table.consumerManager().listAllIds())
                    .anyMatch(id -> id.startsWith("global-index-procedure-it-"));
            assertRemoteTableQuery(table, lookupKey, expected);
            // Closing a synchronous CALL result iterator does not cancel the streaming job. Cancel
            // the actual procedure job explicitly so its owner-scoped tombstone is observable.
            cancelClusterJob(QueryServiceProcedure.IDENTIFIER);
        }
        waitForDescriptor(
                table,
                spec,
                descriptor ->
                        !descriptor.ready()
                                && descriptor.ownerToken().equals(procedureDescriptor.ownerToken()),
                null,
                "procedure cancellation tombstone");
    }

    @Test
    @Timeout(180)
    public void testAutomaticAttemptFailover() throws Exception {
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        String lookupKey = "O1CN-automatic-failover.jpg";
        byte[] expected = "automatic-failover-image".getBytes(StandardCharsets.UTF_8);
        insert(lookupKey, expected);
        buildBTreeIndex();

        FileStoreTable table = paimonTable(TABLE);
        QuerySpec spec = querySpec(table);
        JobClient jobClient = startQueryService(table, 2);
        try {
            GlobalIndexQueryServiceDescriptor beforeFailover =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor -> descriptor.ready() && descriptor.endpoints().length == 2,
                            jobClient,
                            "pre-failover service");
            MiniCluster miniCluster = miniCluster(jobClient);
            miniCluster.terminateTaskManager(0).get(30, TimeUnit.SECONDS);
            try {
                waitForDescriptor(
                        table,
                        spec,
                        descriptor ->
                                !descriptor.ready()
                                        && descriptor
                                                .ownerToken()
                                                .equals(beforeFailover.ownerToken()),
                        jobClient,
                        "same-job failover tombstone");
            } finally {
                miniCluster.startTaskManager();
            }

            GlobalIndexQueryServiceDescriptor recovered =
                    waitForDescriptor(
                            table,
                            spec,
                            descriptor ->
                                    descriptor.ready()
                                            && descriptor.endpoints().length == 2
                                            && !descriptor
                                                    .ownerToken()
                                                    .equals(beforeFailover.ownerToken()),
                            jobClient,
                            "same-job replacement attempt");
            assertThat(recovered.servedSnapshotId()).isEqualTo(beforeFailover.servedSnapshotId());
            assertRemoteTableQuery(table, lookupKey, expected);
        } finally {
            cancel(jobClient);
        }
    }

    private JobClient startQueryService(FileStoreTable table, int parallelism) throws Exception {
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(parallelism)
                        .allowRestart()
                        .build();
        // A caller-supplied generic scan option must not prune the bucket-unaware bootstrap. The
        // query service plans its leased snapshot directly and intentionally ignores scan.bucket.
        FileStoreTable serviceTable =
                (FileStoreTable)
                        table.copy(Collections.singletonMap(CoreOptions.SCAN_BUCKET.key(), "1"));
        QueryService.build(
                env,
                serviceTable,
                parallelism,
                LOOKUP_FIELD,
                Collections.singletonList(VALUE_FIELD),
                CONSUMER_ID,
                LEASE_GRACE_PERIOD);
        return env.executeAsync("global-index-query-service-it-p" + parallelism);
    }

    private GlobalIndexQueryServiceDescriptor waitForDescriptor(
            FileStoreTable table,
            QuerySpec spec,
            Predicate<GlobalIndexQueryServiceDescriptor> condition,
            JobClient jobClient,
            String description)
            throws Exception {
        ServiceManager manager = table.store().newServiceManager();
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        GlobalIndexQueryServiceDescriptor last = null;
        while (System.nanoTime() < deadline) {
            last = manager.globalIndexService(spec.serviceId()).orElse(null);
            if (last != null && condition.test(last)) {
                return last;
            }
            if (jobClient != null) {
                JobStatus status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
                if (status.isTerminalState()) {
                    throw jobFailure(jobClient, status, description, last);
                }
            }
            Thread.sleep(50L);
        }
        fail("Timed out waiting for %s. Last descriptor: %s", description, last);
        return null;
    }

    private void waitForConsumerSnapshot(
            FileStoreTable table, long expectedSnapshotId, JobClient jobClient, String description)
            throws Exception {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            java.util.OptionalLong nextSnapshot = table.consumerManager().minNextSnapshot();
            if (nextSnapshot.isPresent() && nextSnapshot.getAsLong() == expectedSnapshotId) {
                return;
            }
            JobStatus status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status.isTerminalState()) {
                throw jobFailure(jobClient, status, description, null);
            }
            Thread.sleep(50L);
        }
        fail("Timed out waiting for %s.", description);
    }

    private void waitForAnyNewConsumerSnapshot(
            FileStoreTable table,
            Set<String> previousConsumerIds,
            long expectedSnapshotId,
            JobClient jobClient,
            String description)
            throws Exception {
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            boolean advanced =
                    table.consumerManager().listAllIds().stream()
                            .filter(id -> id.startsWith(CONSUMER_ID + '-'))
                            .filter(id -> !previousConsumerIds.contains(id))
                            .anyMatch(
                                    id ->
                                            table.consumerManager()
                                                    .consumer(id)
                                                    .map(
                                                            consumer ->
                                                                    consumer.nextSnapshot()
                                                                            == expectedSnapshotId)
                                                    .orElse(false));
            if (advanced) {
                return;
            }
            JobStatus status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status.isTerminalState()) {
                throw jobFailure(jobClient, status, description, null);
            }
            Thread.sleep(50L);
        }
        fail("Timed out waiting for %s.", description);
    }

    private static AssertionError jobFailure(
            JobClient jobClient,
            JobStatus status,
            String description,
            GlobalIndexQueryServiceDescriptor descriptor) {
        AssertionError failure =
                new AssertionError(
                        String.format(
                                "Query-service job terminated as %s while waiting for %s. Last descriptor: %s",
                                status, description, descriptor));
        try {
            jobClient.getJobExecutionResult().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            failure.initCause(e);
        }
        return failure;
    }

    private void assertBatchLookup(
            FileStoreTable table,
            QuerySpec spec,
            LinkedHashMap<String, byte[]> expected,
            boolean expectBothShards)
            throws Exception {
        List<String> requested = new ArrayList<>(expected.keySet());
        requested.add(1, "missing-object.jpg");
        BinaryRow[] keys =
                requested.stream()
                        .map(GlobalIndexQueryServiceITCase::key)
                        .toArray(BinaryRow[]::new);
        if (expectBothShards) {
            assertThat(
                            Arrays.stream(keys)
                                    .map(
                                            binaryKey ->
                                                    GlobalIndexQueryServiceUtils.route(
                                                            binaryKey, 2))
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(0, 1);
        }

        try (ClientResource client = client(table, spec)) {
            LookupResult result =
                    client.client.getValuesWithMetadata(keys).get(30, TimeUnit.SECONDS);
            GlobalIndexQueryServiceDescriptor advertised =
                    table.store()
                            .newServiceManager()
                            .globalIndexService(spec.serviceId())
                            .orElseThrow(() -> new AssertionError("Missing ready descriptor"));
            assertThat(result.servedGeneration()).isEqualTo(advertised.servedGeneration());
            assertThat(result.servedSnapshotId()).isEqualTo(advertised.servedSnapshotId());
            assertThat(result.snapshotUuid()).isEqualTo(advertised.snapshotUuid());
            BinaryRow[] values = result.values();
            assertThat(values).hasSameSizeAs(keys);
            for (int i = 0; i < requested.size(); i++) {
                String request = requested.get(i);
                if (!expected.containsKey(request)) {
                    assertThat(values[i]).isNull();
                    continue;
                }
                assertThat(values[i]).isNotNull();
                BlobDescriptor descriptor = BlobDescriptor.deserialize(values[i].getBinary(0));
                assertBlobReadable(table, descriptor, expected.get(request));
            }
        }
    }

    private BlobDescriptor lookupDescriptor(FileStoreTable table, QuerySpec spec, String lookupKey)
            throws Exception {
        try (ClientResource client = client(table, spec)) {
            BinaryRow[] values =
                    client.client
                            .getValues(new BinaryRow[] {key(lookupKey)})
                            .get(30, TimeUnit.SECONDS);
            assertThat(values[0]).isNotNull();
            return BlobDescriptor.deserialize(values[0].getBinary(0));
        }
    }

    private void assertBlobReadable(
            FileStoreTable table, BlobDescriptor descriptor, byte[] expected) throws Exception {
        assertThat(BlobDescriptor.deserialize(descriptor.serialize())).isEqualTo(descriptor);
        assertThat(Blob.fromDescriptor(UriReader.fromFile(table.fileIO()), descriptor).toData())
                .isEqualTo(expected);
    }

    private void assertFreshClientNotReady(FileStoreTable table, QuerySpec spec, BinaryRow key)
            throws Exception {
        try (ClientResource client = client(table, spec)) {
            Throwable failure = failure(client.client.getValues(new BinaryRow[] {key}));
            assertThat(failure).isInstanceOf(QueryServiceNotReadyException.class);
        }
    }

    private void assertStaleFenceRejected(GlobalIndexQueryLocation oldFence, BinaryRow key)
            throws Exception {
        try (ClientResource client = new ClientResource(new GlobalIndexQueryClient(oldFence, 1))) {
            Throwable failure = failure(client.client.getValues(new BinaryRow[] {key}));
            assertThat(failure).isInstanceOf(GlobalIndexQueryException.class);
            assertThat(((GlobalIndexQueryException) failure).errorCode())
                    .isEqualTo(STALE_GENERATION);
        }
    }

    private void assertRemoteTableQuery(FileStoreTable table, String lookupKey, byte[] expected)
            throws Exception {
        assertThat(
                        RemoteGlobalIndexTableQuery.isRemoteServiceAvailable(
                                table, LOOKUP_FIELD, Collections.singletonList(VALUE_FIELD)))
                .isTrue();
        RemoteGlobalIndexTableQuery query =
                new RemoteGlobalIndexTableQuery(
                        table, LOOKUP_FIELD, Collections.singletonList(VALUE_FIELD));
        try {
            InternalRow hit =
                    query.lookup(
                            BinaryRow.EMPTY_ROW,
                            0,
                            GenericRow.of(BinaryString.fromString(lookupKey)));
            assertThat(hit).isNotNull();
            assertBlobReadable(table, BlobDescriptor.deserialize(hit.getBinary(0)), expected);
            assertThat(
                            query.lookup(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    GenericRow.of(BinaryString.fromString("missing-object.jpg"))))
                    .isNull();
            assertThat(query.withValueProjection(new int[] {1})).isSameAs(query);
            assertThat(query.createValueSerializer()).isNotNull();
        } finally {
            query.close();
            query.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private GlobalIndexQueryServiceDescriptor waitForActionDescriptor(
            FileStoreTable table,
            QuerySpec spec,
            Predicate<GlobalIndexQueryServiceDescriptor> condition,
            CompletableFuture<Void> actionExecution,
            String description)
            throws Exception {
        ServiceManager manager = table.store().newServiceManager();
        long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
        GlobalIndexQueryServiceDescriptor last = null;
        while (System.nanoTime() < deadline) {
            last = manager.globalIndexService(spec.serviceId()).orElse(null);
            if (last != null && condition.test(last)) {
                return last;
            }
            if (actionExecution.isDone()) {
                actionExecution.get(10, TimeUnit.SECONDS);
                fail(
                        "Action completed before publishing %s. Last descriptor: %s",
                        description, last);
            }
            Thread.sleep(50L);
        }
        fail("Timed out waiting for %s. Last descriptor: %s", description, last);
        return null;
    }

    private static CompletableFuture<Void> runActionAsync(Action action) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                action.run();
                                result.complete(null);
                            } catch (Throwable t) {
                                result.completeExceptionally(t);
                            }
                        },
                        "global-index-query-service-action-it");
        thread.setContextClassLoader(Thread.currentThread().getContextClassLoader());
        thread.setDaemon(true);
        thread.start();
        return result;
    }

    private static void awaitActionTermination(CompletableFuture<Void> actionExecution)
            throws Exception {
        try {
            actionExecution.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException expectedCancellation) {
            // A blocking Action uses env.execute; cancelling its MiniCluster job completes the
            // action exceptionally on some Flink versions and normally on others.
        }
    }

    private void cancelClusterJob(String jobName) throws Exception {
        try (ClusterClient<?> clusterClient = MINI_CLUSTER_EXTENSION.createRestClusterClient()) {
            long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                for (JobStatusMessage job : clusterClient.listJobs().get(10, TimeUnit.SECONDS)) {
                    if (jobName.equals(job.getJobName()) && !job.getJobState().isTerminalState()) {
                        clusterClient.cancel(job.getJobId()).get(30, TimeUnit.SECONDS);
                        return;
                    }
                }
                Thread.sleep(50L);
            }
            fail("Timed out waiting to cancel action job %s.", jobName);
        }
    }

    private static MiniCluster miniCluster(JobClient jobClient) throws Exception {
        Field field = jobClient.getClass().getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(jobClient);
    }

    private ClientResource client(FileStoreTable table, QuerySpec spec) {
        return new ClientResource(
                new GlobalIndexQueryClient(
                        new GlobalIndexQueryLocationImpl(
                                table.store().newServiceManager(),
                                table.uuid(),
                                table.coreOptions().branch(),
                                table.schema().id(),
                                spec),
                        1));
    }

    private GlobalIndexQueryLocation frozenLocation(GlobalIndexQueryServiceDescriptor descriptor) {
        Endpoint[] endpoints = descriptor.endpoints();
        return (key, forceUpdate) -> {
            int shard = GlobalIndexQueryServiceUtils.route(key, endpoints.length);
            Endpoint endpoint = endpoints[shard];
            return new GlobalIndexQueryEndpoint(
                    shard,
                    endpoint.address(),
                    endpoint.serverEpoch(),
                    descriptor.servedGeneration(),
                    descriptor.servedSnapshotId(),
                    descriptor.snapshotUuid());
        };
    }

    private static Throwable failure(java.util.concurrent.CompletableFuture<?> future)
            throws Exception {
        try {
            future.get(30, TimeUnit.SECONDS);
            fail("Expected the query to fail.");
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private LinkedHashMap<String, byte[]> initialRowsCoveringBothShards() {
        LinkedHashMap<String, byte[]> rows = new LinkedHashMap<>();
        int[] perShard = new int[2];
        for (int candidate = 0; perShard[0] < 3 || perShard[1] < 3; candidate++) {
            String lookupKey = String.format("O1CN-object-%03d.jpg", candidate);
            int shard = GlobalIndexQueryServiceUtils.route(key(lookupKey), 2);
            if (perShard[shard] >= 3) {
                continue;
            }
            rows.put(lookupKey, ("image-" + candidate).getBytes(StandardCharsets.UTF_8));
            perShard[shard]++;
        }
        return rows;
    }

    private void insert(String lookupKey, byte[] value) {
        batchSql("INSERT INTO %s VALUES ('%s', X'%s')", TABLE, lookupKey, toHex(value));
    }

    private void buildBTreeIndex() {
        batchSql(
                "CALL sys.create_global_index(`table` => 'default.%s', "
                        + "index_column => '%s', index_type => 'btree')",
                TABLE, LOOKUP_FIELD);
    }

    private long blobFileCount() {
        List<Row> rows =
                batchSql("SELECT COUNT(*) FROM `%s$files` WHERE file_path LIKE '%%.blob'", TABLE);
        return rows.get(0).getFieldAs(0);
    }

    private void expireAllButLatest(FileStoreTable table) {
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMin(1)
                                .snapshotRetainMax(1)
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();
    }

    private static QuerySpec querySpec(FileStoreTable table) {
        return GlobalIndexQueryServiceUtils.querySpec(
                table, LOOKUP_FIELD, Collections.singletonList(VALUE_FIELD));
    }

    private static BinaryRow key(String value) {
        return KEY_SERIALIZER.toBinaryRow(GenericRow.of(BinaryString.fromString(value))).copy();
    }

    private static String toHex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(String.format("%02X", b & 0xff));
        }
        return result.toString();
    }

    private static void cancel(JobClient jobClient) throws Exception {
        JobStatus status = jobClient.getJobStatus().get(10, TimeUnit.SECONDS);
        if (!status.isTerminalState()) {
            jobClient.cancel().get(30, TimeUnit.SECONDS);
        }
    }

    private static class ClientResource implements AutoCloseable {

        private final GlobalIndexQueryClient client;

        private ClientResource(GlobalIndexQueryClient client) {
            this.client = client;
        }

        @Override
        public void close() {
            client.shutdown();
        }
    }
}
