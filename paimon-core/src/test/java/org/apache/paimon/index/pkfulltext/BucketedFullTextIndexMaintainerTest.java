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

package org.apache.paimon.index.pkfulltext;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests file-aligned maintenance of primary-key full-text archives. */
class BucketedFullTextIndexMaintainerTest {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void testReplacesArchiveWithCompactSourceWithoutMergingArchives() throws Exception {
        DataFileMeta oldData = dataFile("old-data");
        DataFileMeta newData = dataFile("new-data");
        IndexFileMeta oldPayload = payload("old-payload", oldData, "fingerprint");
        IndexFileMeta newPayload = payload("new-payload", newData, "fingerprint");
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(newData)).thenReturn(newPayload);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        mock(PkFullTextIndexFile.class),
                        builder,
                        Collections.singletonList(oldData),
                        Collections.singletonList(oldPayload),
                        executor);

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(oldData),
                                Collections.singletonList(newData),
                                Collections.emptyList()),
                        true);

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).containsExactly(newPayload);
        assertThat(commit.compactIncrement().get().deletedIndexFiles()).containsExactly(oldPayload);
        assertThat(maintainer.state().payloadBySourceFile()).containsOnlyKeys("new-data");
        verify(builder).build(newData);
    }

    @Test
    void testMergesArchivesAtConfiguredFanout() throws Exception {
        DataFileMeta first = dataFile("data-1");
        DataFileMeta second = dataFile("data-2");
        IndexFileMeta firstPayload = payload("payload-1", first, "fingerprint");
        IndexFileMeta secondPayload = payload("payload-2", second, "fingerprint");
        IndexFileMeta merged = payload("merged", Arrays.asList(first, second), "fingerprint");
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(Arrays.asList(first, second))).thenReturn(merged);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        mock(PkFullTextIndexFile.class),
                        builder,
                        2,
                        0.5,
                        Arrays.asList(first, second),
                        Arrays.asList(firstPayload, secondPayload),
                        executor);

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles()).containsExactly(merged);
        assertThat(commit.appendIncrement().get().deletedIndexFiles())
                .containsExactly(firstPayload, secondPayload);
        assertThat(maintainer.state().payloadBySourceFile()).containsOnlyKeys("data-1", "data-2");
        assertThat(maintainer.payloads()).containsExactly(merged);
        verify(builder).build(Arrays.asList(first, second));
    }

    @Test
    void testRebuildsArchiveAtConfiguredStaleRatio() throws Exception {
        DataFileMeta active = dataFile("active");
        DataFileMeta removed = dataFile("removed");
        IndexFileMeta oldPayload =
                payload("old-payload", Arrays.asList(active, removed), "fingerprint");
        IndexFileMeta rebuilt = payload("rebuilt", active, "fingerprint");
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(active)).thenReturn(rebuilt);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        mock(PkFullTextIndexFile.class),
                        builder,
                        5,
                        0.5,
                        Collections.singletonList(active),
                        Collections.singletonList(oldPayload),
                        executor);

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles()).containsExactly(rebuilt);
        assertThat(commit.appendIncrement().get().deletedIndexFiles()).containsExactly(oldPayload);
        assertThat(maintainer.payloads()).containsExactly(rebuilt);
        verify(builder).build(active);
    }

    @Test
    void testRebuildsOldFingerprintAndRetriesAfterFailure() throws Exception {
        DataFileMeta data = dataFile("data");
        IndexFileMeta oldPayload = payload("old-payload", data, "old-fingerprint");
        IndexFileMeta newPayload = payload("new-payload", data, "new-fingerprint");
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(data))
                .thenThrow(new IOException("first failure"))
                .thenReturn(newPayload);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "new-fingerprint",
                        mock(PkFullTextIndexFile.class),
                        builder,
                        Collections.singletonList(data),
                        Collections.singletonList(oldPayload),
                        executor);

        assertThatThrownBy(
                        () ->
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(),
                                        CompactIncrement.emptyIncrement(),
                                        true))
                .hasMessageContaining("first failure");

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles()).containsExactly(newPayload);
        assertThat(commit.appendIncrement().get().deletedIndexFiles()).containsExactly(oldPayload);
    }

    @Test
    void testNonBlockingBuildPublishesOnALaterAppendIncrement() throws Exception {
        DataFileMeta data = dataFile("data");
        IndexFileMeta payload = payload("payload", data, "fingerprint");
        CountDownLatch buildStarted = new CountDownLatch(1);
        CountDownLatch allowBuild = new CountDownLatch(1);
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(data))
                .thenAnswer(
                        ignored -> {
                            buildStarted.countDown();
                            assertThat(allowBuild.await(30, TimeUnit.SECONDS)).isTrue();
                            return payload;
                        });
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        mock(PkFullTextIndexFile.class),
                        builder,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);
        try {
            BucketedFullTextIndexMaintainer.FullTextIndexCommit first =
                    maintainer.prepareCommit(
                            DataIncrement.emptyIncrement(),
                            new CompactIncrement(
                                    Collections.emptyList(),
                                    Collections.singletonList(data),
                                    Collections.emptyList()),
                            false);
            assertThat(first.compactIncrement()).isEmpty();
            assertThat(buildStarted.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(maintainer.buildNotCompleted()).isTrue();
            allowBuild.countDown();

            BucketedFullTextIndexMaintainer.FullTextIndexCommit second =
                    maintainer.prepareCommit(
                            DataIncrement.emptyIncrement(),
                            CompactIncrement.emptyIncrement(),
                            true);
            assertThat(second.appendIncrement()).isPresent();
            assertThat(second.appendIncrement().get().newIndexFiles()).containsExactly(payload);
        } finally {
            allowBuild.countDown();
        }
    }

    @Test
    void testCoordinatorAbortRestoresStateAndDeletesGeneratedArchive() throws Exception {
        DataFileMeta data = dataFile("data");
        IndexFileMeta payload = payload("payload", data, "fingerprint");
        PkFullTextIndexFile indexFile = mock(PkFullTextIndexFile.class);
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(data)).thenReturn(payload);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        indexFile,
                        builder,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.emptyList()),
                        true);
        assertThat(maintainer.payloads()).containsExactly(payload);

        commit.abort(new IllegalStateException("later definition failed"));

        assertThat(maintainer.payloads()).isEmpty();
        verify(indexFile).delete(payload);
    }

    @Test
    void testAbortDoesNotDeleteRejectedArchiveTwice() throws Exception {
        DataFileMeta data = dataFile("data");
        IndexFileMeta rejected = payload("rejected", dataFile("other"), "fingerprint");
        IndexFileMeta accepted = payload("accepted", data, "fingerprint");
        PkFullTextIndexFile indexFile = mock(PkFullTextIndexFile.class);
        PkFullTextIndexBuilder builder = mock(PkFullTextIndexBuilder.class);
        when(builder.build(data)).thenReturn(rejected, accepted);
        BucketedFullTextIndexMaintainer maintainer =
                new BucketedFullTextIndexMaintainer(
                        7,
                        "fingerprint",
                        indexFile,
                        builder,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedFullTextIndexMaintainer.FullTextIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.emptyList()),
                        true);

        verify(indexFile).delete(rejected);
        commit.abort(new IllegalStateException("later definition failed"));

        verify(indexFile, times(1)).delete(rejected);
        verify(indexFile).delete(accepted);
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        1,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(1);
    }

    private static IndexFileMeta payload(
            String payloadName, DataFileMeta source, String fingerprint) {
        return payload(payloadName, Collections.singletonList(source), fingerprint);
    }

    private static IndexFileMeta payload(
            String payloadName, List<DataFileMeta> sources, String fingerprint) {
        List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>();
        long rowCount = 0;
        for (DataFileMeta source : sources) {
            sourceFiles.add(new PrimaryKeyIndexSourceFile(source.fileName(), source.rowCount()));
            rowCount += source.rowCount();
        }
        PrimaryKeyIndexSourceMeta sourceMeta =
                new PrimaryKeyIndexSourceMeta(sourceFiles, fingerprint);
        return new IndexFileMeta(
                "full-text",
                payloadName,
                100,
                rowCount,
                new GlobalIndexMeta(0, rowCount - 1, 7, null, null, sourceMeta.serialize()),
                null);
    }
}
