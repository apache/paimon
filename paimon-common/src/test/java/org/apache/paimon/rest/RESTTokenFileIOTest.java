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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RESTTokenFileIO}. */
class RESTTokenFileIOTest {

    @Test
    void testSetFileIOCacheMaximumSize() {
        long originalMaximumSize = RESTTokenFileIO.fileIOCacheMaximumSize();
        try {
            RESTTokenFileIO.setFileIOCacheMaximumSize(2000);
            assertThat(RESTTokenFileIO.fileIOCacheMaximumSize()).isEqualTo(2000);
            assertThatThrownBy(() -> RESTTokenFileIO.setFileIOCacheMaximumSize(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Maximum cache size must be positive.");
        } finally {
            RESTTokenFileIO.setFileIOCacheMaximumSize(originalMaximumSize);
        }
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
    void testPresigningRefreshesForRequestedLifetime() throws IOException {
        checkPresignedLifetime(
                Duration.ofHours(3), Duration.ofHours(2), Duration.ofHours(4), true, false);
    }

    @Test
    void testPresigningReusesSufficientLifetime() throws IOException {
        checkPresignedLifetime(
                Duration.ofMinutes(30), Duration.ofHours(2), Duration.ofHours(4), false, false);
    }

    @Test
    void testPresigningAcceptsWhenOneSecondExceedsValidity() throws IOException {
        checkPresignedLifetime(
                Duration.ofMinutes(90),
                Duration.ofMinutes(90).plusSeconds(1),
                Duration.ofHours(3),
                false,
                false);
    }

    @Test
    void testPresigningRejectsInsufficientRefreshedLifetime() throws IOException {
        checkPresignedLifetime(
                Duration.ofHours(3), Duration.ofHours(2), Duration.ofHours(2), true, true);
    }

    private void checkPresignedLifetime(
            Duration validity,
            Duration initialLifetime,
            Duration refreshedLifetime,
            boolean refresh,
            boolean rejected)
            throws IOException {
        Path root = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data.blob", 0, 1);
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        when(delegate.createBlobPresignedUrl(root, descriptor, validity))
                .thenReturn("https://signed");
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        long now = System.currentTimeMillis();
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap(
                                        "test.token", UUID.randomUUID().toString()),
                                now + initialLifetime.toMillis()),
                        new GetTableTokenResponse(
                                Collections.singletonMap(
                                        "test.token", UUID.randomUUID().toString()),
                                now + refreshedLifetime.toMillis()));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null), api, identifier, root) {
                    @Override
                    long currentTimeMillis() {
                        return now;
                    }
                };
        fileIO.validToken();
        if (rejected) {
            assertThatThrownBy(() -> fileIO.createBlobPresignedUrl(root, descriptor, validity))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("credential lifetime after refresh");
            verify(delegate, never()).createBlobPresignedUrl(any(), any(), any());
        } else {
            assertThat(fileIO.createBlobPresignedUrl(root, descriptor, validity))
                    .isEqualTo("https://signed");
            verify(delegate).createBlobPresignedUrl(root, descriptor, validity);
        }
        verify(api, times(refresh ? 2 : 1)).loadTableToken(identifier);
    }

    @Test
    void testPresigningRefreshesAndResignsAfterMaterialization() throws IOException {
        Path root = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data.blob", 0, 1);
        Duration validity = Duration.ofMinutes(30);
        AtomicLong now = new AtomicLong(1700000000000L);
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        when(delegate.createBlobPresignedUrl(root, descriptor, validity))
                .thenAnswer(
                        ignored -> {
                            if (now.get() == 1700000000000L) {
                                now.addAndGet(Duration.ofHours(2).toMillis());
                                return "https://first";
                            }
                            return "https://refreshed";
                        });
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(
                                Collections.singletonMap(
                                        "test.token", UUID.randomUUID().toString()),
                                now.get() + Duration.ofHours(2).toMillis()),
                        new GetTableTokenResponse(
                                Collections.singletonMap(
                                        "test.token", UUID.randomUUID().toString()),
                                now.get() + Duration.ofHours(4).toMillis()));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null), api, identifier, root) {
                    @Override
                    long currentTimeMillis() {
                        return now.get();
                    }
                };

        fileIO.validToken();
        assertThat(fileIO.createBlobPresignedUrl(root, descriptor, validity))
                .isEqualTo("https://refreshed");
        verify(delegate, times(2)).createBlobPresignedUrl(root, descriptor, validity);
        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testFileIOCreationFailureSurfacesAsCheckedIOException() throws IOException {
        Path tableRoot = new Path("resttoken-broken://bucket/table");
        // the loader's access check fails, so FileIO.get cannot produce an inner FileIO
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenThrow(new IOException("token fs unavailable"));
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.getScheme()).thenReturn("resttoken-broken");
        when(loader.load(any())).thenReturn(delegate);
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        // a unique token, so the static token-keyed FileIO cache cannot serve another test's
        // delegate and the creation path actually runs
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

        // FileIO operations declare IOException; failing to create the inner FileIO must
        // surface the same way instead of bypassing callers as UncheckedIOException
        assertThatThrownBy(() -> fileIO.exists(tableRoot)).isInstanceOf(IOException.class);
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

    private static FileIOLoader ossLoader() throws IOException {
        FileIO delegate = mock(FileIO.class);
        when(delegate.exists(any())).thenReturn(true);
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenReturn(delegate);
        when(loader.getScheme()).thenReturn("oss");
        return loader;
    }

    private static RESTApi apiVendingTokenWithRemaining(
            Identifier identifier, long remainingMillis) {
        RESTApi api = mock(RESTApi.class);
        when(api.loadTableToken(identifier))
                .thenAnswer(
                        invocation ->
                                new GetTableTokenResponse(
                                        Collections.emptyMap(),
                                        System.currentTimeMillis() + remainingMillis));
        return api;
    }

    @Test
    void testDefaultWindowRefreshesOnceWhileTheTokenHasMoreThanFiveMinutesLeft()
            throws IOException {
        Identifier identifier = Identifier.create("db", "table");
        RESTApi api = apiVendingTokenWithRemaining(identifier, Duration.ofMinutes(30).toMillis());
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), ossLoader(), null),
                        api,
                        identifier,
                        new Path("oss://bucket/table"));

        fileIO.exists(new Path("oss://bucket/table/a"));
        fileIO.exists(new Path("oss://bucket/table/b"));
        fileIO.exists(new Path("oss://bucket/table/c"));

        verify(api, times(1)).loadTableToken(identifier);
    }

    @Test
    void testDefaultWindowRefreshesATokenWithLessThanFiveMinutesLeftOnEveryAccess()
            throws IOException {
        Identifier identifier = Identifier.create("db", "table");
        RESTApi api = apiVendingTokenWithRemaining(identifier, Duration.ofMinutes(2).toMillis());
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), ossLoader(), null),
                        api,
                        identifier,
                        new Path("oss://bucket/table"));

        fileIO.exists(new Path("oss://bucket/table/a"));
        fileIO.exists(new Path("oss://bucket/table/b"));

        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testConfiguredWindowAboveTheTokenLifetimeRefreshesOnEveryAccess() throws IOException {
        Identifier identifier = Identifier.create("db", "table");
        RESTApi api = apiVendingTokenWithRemaining(identifier, Duration.ofMinutes(30).toMillis());
        Options options = new Options();
        options.set(RESTCatalogOptions.DATA_TOKEN_EXPIRATION_SAFE_TIME, Duration.ofHours(1));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(options, ossLoader(), null),
                        api,
                        identifier,
                        new Path("oss://bucket/table"));

        fileIO.exists(new Path("oss://bucket/table/a"));
        fileIO.exists(new Path("oss://bucket/table/b"));

        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testConfiguredWindowBelowTheTokenLifetimeRefreshesOnce() throws IOException {
        Identifier identifier = Identifier.create("db", "table");
        RESTApi api = apiVendingTokenWithRemaining(identifier, Duration.ofMinutes(2).toMillis());
        Options options = new Options();
        options.set(RESTCatalogOptions.DATA_TOKEN_EXPIRATION_SAFE_TIME, Duration.ofMinutes(1));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(options, ossLoader(), null),
                        api,
                        identifier,
                        new Path("oss://bucket/table"));

        fileIO.exists(new Path("oss://bucket/table/a"));
        fileIO.exists(new Path("oss://bucket/table/b"));

        verify(api, times(1)).loadTableToken(identifier);
    }

    @Test
    void testRefreshWindowSurvivesSerialization() throws IOException, ClassNotFoundException {
        Identifier identifier = Identifier.create("db", "table");
        Options options = new Options();
        options.set(RESTCatalogOptions.DATA_TOKEN_EXPIRATION_SAFE_TIME, Duration.ofMinutes(7));
        RESTTokenFileIO fileIO =
                new RESTTokenFileIO(
                        CatalogContext.create(options, null, null),
                        null,
                        identifier,
                        new Path("oss://bucket/table"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(fileIO);
        }
        RESTTokenFileIO copy;
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            copy = (RESTTokenFileIO) in.readObject();
        }

        assertThat(copy.expirationSafeTimeMillis()).isEqualTo(Duration.ofMinutes(7).toMillis());
    }

    @Test
    void testNegativeRefreshWindowIsRejected() {
        Options options = new Options();
        options.set(RESTCatalogOptions.DATA_TOKEN_EXPIRATION_SAFE_TIME, Duration.ofMinutes(-1));
        assertThatThrownBy(
                        () ->
                                new RESTTokenFileIO(
                                        CatalogContext.create(options, null, null),
                                        null,
                                        Identifier.create("db", "table"),
                                        new Path("oss://bucket/table")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data-token.expiration-safe-time");
    }
}
