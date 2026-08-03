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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.BaseMultiPartUploadCommitter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RESTTokenFileIO}. */
class RESTTokenFileIOTest {

    private static final Path TABLE_ROOT = new Path("oss://bucket/table");
    private static final Path FILE = new Path("oss://bucket/table/bucket-0/data");

    /** The cache hands releases to an executor, so a close lands shortly after, not inline. */
    private static final long CLOSED_MILLIS = 30_000;

    private static final long NOT_CLOSED_MILLIS = 500;

    @BeforeEach
    @AfterEach
    void clearFileIOCache() {
        // the cache is static and shared by every test in this fork
        RESTTokenFileIO.invalidateFileIOCache();
    }

    @Test
    void testCreateBlobPresignedUrlRequiresBoundRootAndDelegates() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(5);
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        when(delegate.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .thenReturn("https://example");
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier))
                .thenReturn(new GetTableTokenResponse(Collections.emptyMap(), Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, validity))
                .isEqualTo("https://example");
        verify(delegate).createBlobPresignedUrl(tableRoot, descriptor, validity);

        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        new Path("oss://bucket/other"), descriptor, validity))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bound table root");
    }

    @Test
    void testTryToWriteAtomicReachesInnerOverride() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        FileIO delegate = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a unique token, so the static token-keyed FileIO cache cannot serve another test's
        // delegate
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap("token", UUID.randomUUID().toString()),
                                Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);

        Path target = new Path("oss://bucket/table/snapshot/LATEST");
        when(delegate.tryToWriteAtomic(target, "content")).thenReturn(true);

        assertThat(fileIO.tryToWriteAtomic(target, "content")).isTrue();
        verify(delegate).tryToWriteAtomic(target, "content");
        // the interface default would have written a temp file and renamed it instead
        verify(delegate, never()).rename(any(), any());
    }

    @Test
    void testListFilesIterativeReachesInnerOverride() throws IOException {
        Path tableRoot = new Path("oss://bucket/table");
        FileIO delegate = mock(FileIO.class);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a unique token, so the static token-keyed FileIO cache cannot serve another test's
        // delegate
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap("token", UUID.randomUUID().toString()),
                                Long.MAX_VALUE));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null),
                        api,
                        identifier,
                        tableRoot);
        FileStatus status = mock(FileStatus.class);
        RemoteIterator<FileStatus> iterator =
                new RemoteIterator<FileStatus>() {
                    private boolean emitted;

                    @Override
                    public boolean hasNext() {
                        return !emitted;
                    }

                    @Override
                    public FileStatus next() {
                        emitted = true;
                        return status;
                    }
                };
        when(delegate.listFilesIterative(tableRoot, false)).thenReturn(iterator);

        RemoteIterator<FileStatus> actual = fileIO.listFilesIterative(tableRoot, false);

        assertThat(actual.hasNext()).isTrue();
        assertThat(actual.next()).isSameAs(status);
        assertThat(actual.hasNext()).isFalse();
        verify(delegate).listFilesIterative(tableRoot, false);
        // the interface default would construct its own iterator backed by listStatus
        verify(delegate, never()).listStatus(any());
    }

    @Test
    void testEvictionKeepsALeasedFileIOAlive() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("leased", delegates);

        RESTTokenFileIO.Lease lease = fileIO.acquire();
        FileIO delegate = delegates.get(0);
        assertThat(lease.fileIO()).isSameAs(delegate);

        RESTTokenFileIO.invalidateFileIOCache();

        // the cache handed its own reference back, ours is still out
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();
        assertThat(lease.fileIO().exists(FILE)).isTrue();

        lease.close();

        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testEvictionClosesAnUnleasedFileIO() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("unleased", delegates);

        fileIO.exists(FILE);

        RESTTokenFileIO.invalidateFileIOCache();

        verify(delegates.get(0), timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testAnOpenStreamKeepsTheFileIOAlive() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("stream", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        SeekableInputStream delegateStream = mock(SeekableInputStream.class);
        when(delegate.newInputStream(FILE)).thenReturn(delegateStream);

        SeekableInputStream in = fileIO.newInputStream(FILE);
        RESTTokenFileIO.invalidateFileIOCache();

        // a read in flight outlives the eviction that dropped its cache entry
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();

        in.close();

        verify(delegateStream, times(1)).close();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testAnOutputStreamKeepsTheFileIOAlive() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("output", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        PositionOutputStream delegateStream = mock(PositionOutputStream.class);
        when(delegate.newOutputStream(FILE, false)).thenReturn(delegateStream);

        PositionOutputStream out = fileIO.newOutputStream(FILE, false);
        RESTTokenFileIO.invalidateFileIOCache();

        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();

        out.close();

        verify(delegateStream, times(1)).close();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testTheLeasedStreamKeepsTheVectoredReadCapability() throws Exception {
        // readers choose their strategy with instanceof VectoredReadable, so a wrapper that hides
        // it would silently downgrade every REST catalog read to sequential
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("vectored", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        VectoredSeekableInputStream delegateStream = mock(VectoredSeekableInputStream.class);
        when(delegate.newInputStream(FILE)).thenReturn(delegateStream);
        when(delegateStream.pread(1L, new byte[0], 2, 3)).thenReturn(7);
        when(delegateStream.parallelismForVectorReads()).thenReturn(11);

        SeekableInputStream in = fileIO.newInputStream(FILE);
        assertThat(in).isInstanceOf(VectoredReadable.class);
        VectoredReadable vectored = (VectoredReadable) in;
        assertThat(vectored.pread(1L, new byte[0], 2, 3)).isEqualTo(7);
        assertThat(vectored.parallelismForVectorReads()).isEqualTo(11);

        // the capability-carrying wrapper still has to hand its lease back
        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();
        in.close();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testAPlainStreamIsNotDressedUpAsVectored() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("plainstream", delegates);
        fileIO.exists(FILE);
        when(delegates.get(0).newInputStream(FILE)).thenReturn(mock(SeekableInputStream.class));

        try (SeekableInputStream in = fileIO.newInputStream(FILE)) {
            assertThat(in).isNotInstanceOf(VectoredReadable.class);
        }
    }

    @Test
    void testATwoPhaseStreamReleasesItsLeaseOnCloseForCommit() throws Exception {
        // the load-bearing one: FormatTableSingleFileWriter only ever calls closeForCommit()
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("twophasecommit", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        when(delegate.newTwoPhaseOutputStream(FILE, false))
                .thenReturn(mock(TwoPhaseOutputStream.class));

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(FILE, false);
        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();

        out.closeForCommit();

        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testATwoPhaseStreamReleasesItsLeaseOnClose() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("twophaseclose", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        when(delegate.newTwoPhaseOutputStream(FILE, false))
                .thenReturn(mock(TwoPhaseOutputStream.class));

        TwoPhaseOutputStream out = fileIO.newTwoPhaseOutputStream(FILE, false);
        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();

        out.close();

        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testAFailedOpenHandsTheLeaseBack() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("failedopen", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);
        when(delegate.newInputStream(FILE)).thenThrow(new IOException("cannot open"));

        assertThatThrownBy(() -> fileIO.newInputStream(FILE)).isInstanceOf(IOException.class);

        // a lease stranded by the failure would keep the eviction from ever closing the delegate
        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testAClosedFileIOIsRebuiltRatherThanHandedOut() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("rebuild", delegates);

        fileIO.exists(FILE);
        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegates.get(0), timeout(CLOSED_MILLIS).times(1)).close();

        fileIO.exists(FILE);

        assertThat(delegates).hasSize(2);
        verify(delegates.get(1), never()).close();
    }

    @Test
    void testClosingALeaseTwiceReleasesOnce() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("idempotent", delegates);

        RESTTokenFileIO.Lease lease = fileIO.acquire();
        lease.close();
        lease.close();

        // nothing was evicted yet, so an over-release is the only way this could have closed
        verify(delegates.get(0), after(NOT_CLOSED_MILLIS).never()).close();

        RESTTokenFileIO.invalidateFileIOCache();
        verify(delegates.get(0), timeout(CLOSED_MILLIS).times(1)).close();
    }

    @Test
    void testHolderRefusesToHandOutAReleasedFileIO() throws IOException {
        FileIO delegate = mock(FileIO.class);
        RESTTokenFileIO.CachedFileIO cached = new RESTTokenFileIO.CachedFileIO(delegate);

        RESTTokenFileIO.Lease lease = cached.acquire();
        assertThat(lease).isNotNull();

        // the reference the cache owns, handed back the way an eviction does
        cached.release();
        verify(delegate, after(NOT_CLOSED_MILLIS).never()).close();

        lease.close();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();

        // spent: a lookup that raced with the eviction has to be told to build a new one
        assertThat(cached.acquire()).isNull();
    }

    @Test
    void testLeaseOnAPlainFileIODoesNothing() throws IOException {
        FileIO plain = mock(FileIO.class);

        try (RESTTokenFileIO.Lease lease = RESTTokenFileIO.lease(plain)) {
            assertThat(lease.fileIO()).isSameAs(plain);
        }

        verify(plain, never()).close();
    }

    @Test
    void testMultiPartUploadCommitterHoldsALeaseAcrossTheCall() throws Exception {
        List<FileIO> delegates = new ArrayList<>();
        RESTTokenFileIO fileIO = fileIO("committer", delegates);
        fileIO.exists(FILE);
        FileIO delegate = delegates.get(0);

        @SuppressWarnings("unchecked")
        MultiPartUploadStore<String, String> store = mock(MultiPartUploadStore.class);
        AtomicBoolean closedDuringUpload = new AtomicBoolean();
        when(store.completeMultipartUpload(any(), any(), any(), anyLong()))
                .thenAnswer(
                        ignored -> {
                            // the entry goes away while the upload is being completed
                            RESTTokenFileIO.invalidateFileIOCache();
                            Thread.sleep(NOT_CLOSED_MILLIS);
                            closedDuringUpload.set(
                                    Mockito.mockingDetails(delegate).getInvocations().stream()
                                            .anyMatch(
                                                    i -> "close".equals(i.getMethod().getName())));
                            return "done";
                        });

        TestCommitter<String, String> committer = new TestCommitter<>(store, FILE);
        committer.commit(fileIO);

        verify(store, times(1)).completeMultipartUpload(any(), any(), any(), anyLong());
        assertThat(closedDuringUpload).isFalse();
        verify(delegate, timeout(CLOSED_MILLIS).times(1)).close();
        // the store must be built on the delegate, plugin committers cast it to their own impl
        assertThat(committer.received).isSameAs(delegate);
    }

    private RESTTokenFileIO fileIO(String tokenValue, List<FileIO> delegates) {
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.getScheme()).thenReturn("oss");
        when(loader.load(any()))
                .thenAnswer(
                        ignored -> {
                            FileIO delegate = mock(FileIO.class);
                            when(delegate.exists(any())).thenReturn(true);
                            delegates.add(delegate);
                            return delegate;
                        });
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a token of its own per test, so the shared cache keeps the entries apart
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap("token", tokenValue), Long.MAX_VALUE));
        return new RESTTokenFileIO(
                CatalogContext.create(new Options(), loader, null), api, identifier, TABLE_ROOT);
    }

    /** The shape of a delegate stream whose capability the leased wrapper has to carry through. */
    private abstract static class VectoredSeekableInputStream extends SeekableInputStream
            implements VectoredReadable {}

    private static class TestCommitter<T, C> extends BaseMultiPartUploadCommitter<T, C> {

        private static final long serialVersionUID = 1L;

        private final transient MultiPartUploadStore<T, C> store;

        private transient FileIO received;

        private TestCommitter(MultiPartUploadStore<T, C> store, Path targetPath) {
            super("upload-id", Collections.emptyList(), "object", 0L, targetPath);
            this.store = store;
        }

        @Override
        protected MultiPartUploadStore<T, C> multiPartUploadStore(FileIO fileIO, Path targetPath) {
            received = fileIO;
            return store;
        }
    }
}
