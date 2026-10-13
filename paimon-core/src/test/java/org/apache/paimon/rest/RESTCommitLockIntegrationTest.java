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

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.requests.CommitLockRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.CommitLockResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises REST leases through the real file-store commit path. */
class RESTCommitLockIntegrationTest {

    @Test
    void disabledFactoryDoesNotRequireRestConfigurationOrWriterIdentity() {
        Lock lock =
                new RESTCatalogLockFactory()
                        .createLock(
                                CatalogLockContext.fromOptions(new Options()),
                                null,
                                null,
                                null,
                                new Options());
        assertThat(lock).isInstanceOf(Lock.EmptyLock.class);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void commitsAcquireBeforeHeadAndReacquireAfterConflict(
            boolean locking, boolean firstPublicationConflicts, @TempDir Path directory)
            throws Exception {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.REST_COMMIT_LOCK_ENABLED, locking);
        tableOptions.set(CoreOptions.COMMIT_MAX_RETRIES, 2);
        tableOptions.set(CoreOptions.COMMIT_MIN_RETRY_WAIT, Duration.ofMillis(1));
        tableOptions.set(CoreOptions.COMMIT_MAX_RETRY_WAIT, Duration.ofMillis(1));
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .options(tableOptions.toMap())
                        .build();
        GetTableResponse tableResponse =
                new GetTableResponse(
                        "table-id",
                        "db",
                        "table$branch_dev",
                        directory.toUri().toString(),
                        false,
                        0,
                        schema,
                        null,
                        0,
                        null,
                        0,
                        null);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Snapshot> head = new AtomicReference<>();
        AtomicBoolean lease = new AtomicBoolean();
        AtomicInteger publications = new AtomicInteger();
        try (MockWebServer server = new MockWebServer()) {
            server.setDispatcher(
                    new Dispatcher() {
                        @Override
                        public MockResponse dispatch(RecordedRequest request) {
                            String path = request.getPath();
                            assertThat(request.getRequestUrl().pathSegments())
                                    .contains("table$branch_dev");
                            if (path.endsWith("/commit-lock")) {
                                assertThat(locking).isTrue();
                                CommitLockRequest acquire =
                                        fromJson(request, CommitLockRequest.class);
                                assertThat(acquire.getTableId()).isEqualTo("table-id");
                                assertThat(acquire.getCommitUser()).isEqualTo("morax-job");
                                events.add("acquire");
                                lease.set(true);
                                return json(
                                        new CommitLockResponse(
                                                true, "morax-job", 100000, 60000, head.get()));
                            }
                            if (path.endsWith("/snapshot")) {
                                if (locking) {
                                    assertThat(lease.get()).isTrue();
                                }
                                events.add("head");
                                Snapshot snapshot = head.get();
                                return json(
                                        new GetTableSnapshotResponse(
                                                snapshot == null
                                                        ? null
                                                        : new TableSnapshot(snapshot, 0, 0, 0, 0)));
                            }
                            if (path.endsWith("/commit")) {
                                if (locking) {
                                    assertThat(lease.get()).isTrue();
                                }
                                CommitTableRequest commit =
                                        fromJson(request, CommitTableRequest.class);
                                assertThat(commit.getTableId()).isEqualTo("table-id");
                                assertThat(commit.getSnapshot().commitUser())
                                        .isEqualTo("morax-job");
                                Snapshot previous = head.get();
                                assertThat(commit.getBaseSnapshotUuid())
                                        .isEqualTo(previous == null ? null : previous.uuid());
                                events.add("publish");
                                if (firstPublicationConflicts
                                        && publications.getAndIncrement() == 0) {
                                    return json(Collections.singletonMap("success", false));
                                }
                                head.set(commit.getSnapshot());
                                lease.set(false);
                                return json(Collections.singletonMap("success", true));
                            }
                            if ("GET".equals(request.getMethod()) && !path.contains("/schemas/")) {
                                return json(tableResponse);
                            }
                            return new MockResponse().setResponseCode(400);
                        }
                    });
            server.start();
            Options catalogOptions = new Options();
            catalogOptions.set(RESTCatalogOptions.URI, server.url("/").toString());
            catalogOptions.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
            catalogOptions.set(RESTCatalogOptions.TOKEN, "token");
            catalogOptions.set(RESTTokenFileIO.DATA_TOKEN_ENABLED, false);
            catalogOptions.set(RESTCatalogInternalOptions.PREFIX, "catalog");
            try (RESTCatalog catalog =
                    new RESTCatalog(CatalogContext.create(catalogOptions), false)) {
                FileStoreTable table =
                        (FileStoreTable) catalog.getTable(new Identifier("db", "table", "dev"));
                new FileSystemSchemaManager(table.fileIO(), table.location(), "dev")
                        .createTable(schema);
                try (FileStoreCommit commit =
                        table.store().newCommit("morax-job", table).ignoreEmptyCommit(false)) {
                    assertThat(commit.commit(new ManifestCommittable(1L), false)).isEqualTo(1);
                    assertThat(head.get().id()).isEqualTo(1L);
                    assertThat(events)
                            .containsExactlyElementsOf(
                                    expected(locking, firstPublicationConflicts));
                    events.clear();
                    assertThat(commit.commit(new ManifestCommittable(2L), false)).isEqualTo(1);
                    assertThat(head.get().id()).isEqualTo(2L);
                    assertThat(events).containsExactlyElementsOf(expected(locking, false));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void compactionConflictRollsBackWithWriterContextAndReacquires(
            boolean locking, @TempDir Path directory) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.REST_COMMIT_LOCK_ENABLED.key(), String.valueOf(locking))
                        .option(CoreOptions.COMMIT_MIN_RETRY_WAIT.key(), "1 ms")
                        .option(CoreOptions.COMMIT_MAX_RETRY_WAIT.key(), "1 ms")
                        .build();
        GetTableResponse tableResponse =
                new GetTableResponse(
                        "branch-id",
                        "db",
                        "table$branch_dev",
                        directory.toUri().toString(),
                        false,
                        0,
                        schema,
                        null,
                        0,
                        null,
                        0,
                        null);
        AtomicReference<Snapshot> head = new AtomicReference<>();
        Map<Long, Snapshot> snapshots = new ConcurrentHashMap<>();
        AtomicReference<String> owner = new AtomicReference<>();
        AtomicReference<RollbackTableRequest> rollback = new AtomicReference<>();
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        try (MockWebServer server = new MockWebServer()) {
            server.setDispatcher(
                    new Dispatcher() {
                        @Override
                        public MockResponse dispatch(RecordedRequest request) {
                            String path = request.getPath();
                            assertThat(request.getRequestUrl().pathSegments())
                                    .contains("table$branch_dev");
                            if (path.endsWith("/commit-lock")) {
                                CommitLockRequest acquire =
                                        fromJson(request, CommitLockRequest.class);
                                assertThat(acquire.getTableId()).isEqualTo("branch-id");
                                assertThat(locking).isTrue();
                                owner.set(acquire.getCommitUser());
                                events.add("acquire");
                                return json(
                                        new CommitLockResponse(
                                                true, owner.get(), 100000, 60000, head.get()));
                            }
                            if (path.endsWith("/snapshot")) {
                                events.add("head");
                                Snapshot snapshot = head.get();
                                return json(
                                        new GetTableSnapshotResponse(
                                                snapshot == null
                                                        ? null
                                                        : new TableSnapshot(snapshot, 0, 0, 0, 0)));
                            }
                            if (path.endsWith("/rollback")) {
                                RollbackTableRequest requestBody =
                                        fromJson(request, RollbackTableRequest.class);
                                assertThat(requestBody.getTableId()).isEqualTo("branch-id");
                                assertThat(requestBody.getFromSnapshot())
                                        .isEqualTo(head.get().id());
                                assertThat(requestBody.getCommitUser()).isEqualTo(owner.get());
                                if (locking) {
                                    assertThat(owner.get()).isNotNull();
                                }
                                head.set(
                                        snapshots.get(
                                                ((Instant.SnapshotInstant) requestBody.getInstant())
                                                        .getSnapshotId()));
                                rollback.set(requestBody);
                                owner.set(null);
                                events.add("rollback");
                                return json(Collections.emptyMap());
                            }
                            if (path.endsWith("/commit")) {
                                CommitTableRequest commit =
                                        fromJson(request, CommitTableRequest.class);
                                assertThat(commit.getTableId()).isEqualTo("branch-id");
                                if (locking) {
                                    assertThat(commit.getSnapshot().commitUser())
                                            .isEqualTo(owner.get());
                                }
                                Snapshot previous = head.get();
                                assertThat(commit.getBaseSnapshotUuid())
                                        .isEqualTo(previous == null ? null : previous.uuid());
                                head.set(commit.getSnapshot());
                                snapshots.put(commit.getSnapshot().id(), commit.getSnapshot());
                                owner.set(null);
                                events.add("publish");
                                return json(Collections.singletonMap("success", true));
                            }
                            if ("GET".equals(request.getMethod()) && !path.contains("/schemas/")) {
                                return json(tableResponse);
                            }
                            return new MockResponse().setResponseCode(400);
                        }
                    });
            server.start();
            Options catalogOptions = new Options();
            catalogOptions.set(RESTCatalogOptions.URI, server.url("/").toString());
            catalogOptions.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
            catalogOptions.set(RESTCatalogOptions.TOKEN, "token");
            catalogOptions.set(RESTTokenFileIO.DATA_TOKEN_ENABLED, false);
            catalogOptions.set(RESTCatalogInternalOptions.PREFIX, "catalog");
            try (RESTCatalog catalog =
                    new RESTCatalog(CatalogContext.create(catalogOptions), false)) {
                FileStoreTable table =
                        (FileStoreTable) catalog.getTable(new Identifier("db", "table", "dev"));
                new FileSystemSchemaManager(table.fileIO(), table.location(), "dev")
                        .createTable(schema);
                BatchWriteBuilder builder = table.newBatchWriteBuilder();
                List<DataFileMeta> files = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    try (BatchTableWrite write = builder.newWrite();
                            BatchTableCommit commit = builder.newCommit()) {
                        write.write(GenericRow.of(i));
                        List<CommitMessage> messages = write.prepareCommit();
                        commit.commit(messages);
                        files.add(
                                ((CommitMessageImpl) messages.get(0))
                                        .newFilesIncrement()
                                        .newFiles()
                                        .get(0));
                    }
                }
                CommitMessageImpl deletion =
                        new CommitMessageImpl(
                                EMPTY_ROW,
                                0,
                                -1,
                                new DataIncrement(
                                        Collections.emptyList(),
                                        Collections.singletonList(files.get(0)),
                                        Collections.emptyList()),
                                new CompactIncrement(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList()));
                CommitMessage compacted;
                try (BatchTableWrite write = builder.newWrite()) {
                    FileStoreWrite<?> storeWrite = ((TableWriteImpl<?>) write).getWrite();
                    compacted =
                            new AppendCompactTask(EMPTY_ROW, files)
                                    .doCompact(table, (BaseAppendFileStoreWrite) storeWrite);
                }
                try (BatchTableCommit commit = builder.newCommit()) {
                    commit.commit(Collections.singletonList(compacted));
                }
                Snapshot compactHead = head.get();
                assertThat(compactHead.id()).isEqualTo(6L);
                assertThat(compactHead.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
                events.clear();
                try (BatchTableCommit commit = builder.newCommit()) {
                    commit.commit(Collections.singletonList(deletion));
                }
                Snapshot replacement = head.get();
                assertThat(replacement.id()).isEqualTo(6L);
                assertThat(replacement.uuid()).isNotEqualTo(compactHead.uuid());
                assertThat(replacement.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);
                assertThat(rollback.get().getFromSnapshot()).isEqualTo(6L);
                assertThat(((Instant.SnapshotInstant) rollback.get().getInstant()).getSnapshotId())
                        .isEqualTo(5L);
                assertThat(rollback.get().getCommitUser())
                        .isEqualTo(locking ? replacement.commitUser() : null);
                if (locking) {
                    assertThat(events)
                            .containsSubsequence(
                                    "acquire", "head", "rollback", "acquire", "head", "publish");
                } else {
                    assertThat(events)
                            .containsSubsequence("head", "rollback", "head", "publish")
                            .doesNotContain("acquire");
                }
                List<Integer> values = new ArrayList<>();
                ReadBuilder read = table.newReadBuilder();
                try (RecordReader<InternalRow> reader =
                        read.newRead().createReader(read.newScan().plan())) {
                    reader.forEachRemaining(row -> values.add(row.getInt(0)));
                }
                assertThat(values).containsExactlyInAnyOrder(1, 2, 3, 4);
            }
        }
    }

    private static List<String> expected(boolean locking, boolean retry) {
        List<String> events = new ArrayList<>();
        for (int attempt = 0; attempt < (retry ? 2 : 1); attempt++) {
            if (locking) {
                events.add("acquire");
            }
            events.add("head");
            events.add("publish");
        }
        return events;
    }

    private static <T> T fromJson(RecordedRequest request, Class<T> type) {
        try {
            return RESTApi.fromJson(request.getBody().readUtf8(), type);
        } catch (JsonProcessingException e) {
            throw new AssertionError("Invalid request JSON", e);
        }
    }

    private static MockResponse json(Object body) {
        try {
            return new MockResponse()
                    .setResponseCode(200)
                    .setBody(RESTApi.toJson(body))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new AssertionError("Invalid response JSON", e);
        }
    }
}
