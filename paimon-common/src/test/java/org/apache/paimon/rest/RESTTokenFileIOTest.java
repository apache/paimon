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
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.cache.CachePolicyFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ORIGIN_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_VPC_VALIDATED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;
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
    void testTokenRefreshRebuildsPolicyWrapper() throws IOException {
        Path root = new Path("oss://bucket/table");
        Path data = new Path(root, "bucket-0/data.orc");
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenAnswer(ignored -> mock(FileIO.class));
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        Map<String, String> tokenOptions = new HashMap<>();
        tokenOptions.put(IO_CACHE_POLICY.key(), "read");
        tokenOptions.put(IO_CACHE_VPC_VALIDATED.key(), "true");
        tokenOptions.put(IO_CACHE_ENDPOINT.key(), "cache");
        tokenOptions.put(IO_CACHE_ORIGIN_ENDPOINT.key(), "origin");
        Map<String, String> disabledOptions = new HashMap<>(tokenOptions);
        disabledOptions.put(IO_CACHE_POLICY.key(), "none");
        long now = System.currentTimeMillis();
        AtomicLong clock = new AtomicLong(now);
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(tokenOptions, now + 3_600_000),
                        new GetTableTokenResponse(disabledOptions, Long.MAX_VALUE));
        RESTTokenFileIO rest =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null), api, identifier, root) {
                    @Override
                    long currentTimeMillis() {
                        return clock.get();
                    }
                };
        FileIO first = rest.fileIO();
        assertThat(first).isInstanceOf(CachePolicyFileIO.class);
        assertThat(
                        ((HadoopOptionsProvider) first)
                                .hadoopOptions(data, "read")
                                .get("fs.oss.endpoint"))
                .isEqualTo("cache");
        assertThat(rest.fileIO()).isSameAs(first);
        clock.set(now + 3_600_000);
        FileIO refreshed = rest.fileIO();
        assertThat(refreshed).isNotSameAs(first).isNotInstanceOf(CachePolicyFileIO.class);
        verify(api, times(2)).loadTableToken(identifier);
    }

    @Test
    void testIdenticalTokensSharePolicyWrapperAcrossTablesAndContexts() throws IOException {
        Path firstRoot = new Path("oss://bucket/table-one");
        Path secondRoot = new Path("oss://bucket/table-two");
        Path firstData = new Path(firstRoot, "bucket-0/data.orc");
        Path secondData = new Path(secondRoot, "bucket-0/data.orc");
        RESTApi api = mock(RESTApi.class);
        Map<String, String> tokenOptions = new HashMap<>();
        tokenOptions.put("test.token", UUID.randomUUID().toString());
        tokenOptions.put(IO_CACHE_POLICY.key(), "read");
        tokenOptions.put(IO_CACHE_VPC_VALIDATED.key(), "true");
        tokenOptions.put(IO_CACHE_ENDPOINT.key(), "cache");
        tokenOptions.put(IO_CACHE_ORIGIN_ENDPOINT.key(), "origin");
        tokenOptions.put(IO_CACHE_WHITELIST_PATH.key(), "table-two");
        when(api.loadTableToken(any()))
                .thenReturn(new GetTableTokenResponse(tokenOptions, Long.MAX_VALUE));
        FileIO origin = mock(FileIO.class);
        FileIO cache = mock(FileIO.class);
        FileIOLoader firstLoader = mock(FileIOLoader.class);
        FileIOLoader secondLoader = mock(FileIOLoader.class);
        // FileIO.get probes the loader before creating the actual delegate for each endpoint.
        when(firstLoader.load(any())).thenReturn(origin, origin, cache, cache);
        CatalogContext firstContext = CatalogContext.create(new Options(), firstLoader, null);
        CatalogContext secondContext = CatalogContext.create(new Options(), secondLoader, null);
        RESTTokenFileIO first =
                new RESTTokenFileIO(
                        firstContext, api, Identifier.create("db", "table-one"), firstRoot);
        RESTTokenFileIO second =
                new RESTTokenFileIO(
                        secondContext, api, Identifier.create("db", "table-two"), secondRoot);
        FileIO shared = first.fileIO();
        assertThat(shared).isInstanceOf(CachePolicyFileIO.class);
        assertThat(second.fileIO()).isSameAs(shared);
        first.newInputStream(firstData);
        // Initialize cache only after another table has reused the wrapper. Route its actual path.
        second.newInputStream(secondData);
        verify(origin).newInputStream(firstData);
        verify(cache).newInputStream(secondData);
        assertThat(
                        ((HadoopOptionsProvider) shared)
                                .hadoopOptions(firstData, "read")
                                .get("fs.oss.endpoint"))
                .isEqualTo("origin");
        assertThat(
                        ((HadoopOptionsProvider) shared)
                                .hadoopOptions(secondData, "read")
                                .get("fs.oss.endpoint"))
                .isEqualTo("cache");
        verify(secondLoader, never()).load(any());
    }

    @Test
    void testIdenticalCredentialsWithDifferentCacheOptionsDoNotShareFileIO() throws IOException {
        Path root = new Path("oss://bucket/table");
        Path data = new Path(root, "bucket-0/data.orc");
        Map<String, String> tokenOptions = new HashMap<>();
        tokenOptions.put("fs.oss.securityToken", UUID.randomUUID().toString());
        tokenOptions.put(IO_CACHE_POLICY.key(), "read");
        tokenOptions.put(IO_CACHE_VPC_VALIDATED.key(), "true");
        tokenOptions.put(IO_CACHE_ENDPOINT.key(), "cache");
        tokenOptions.put(IO_CACHE_ORIGIN_ENDPOINT.key(), "origin");
        Map<String, String> disabledOptions = new HashMap<>(tokenOptions);
        disabledOptions.put(IO_CACHE_POLICY.key(), "none");
        Map<String, String> otherEndpointOptions = new HashMap<>(tokenOptions);
        otherEndpointOptions.put(IO_CACHE_ENDPOINT.key(), "other-cache");
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier))
                .thenReturn(
                        new GetTableTokenResponse(tokenOptions, Long.MAX_VALUE),
                        new GetTableTokenResponse(disabledOptions, Long.MAX_VALUE),
                        new GetTableTokenResponse(otherEndpointOptions, Long.MAX_VALUE));
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.load(any())).thenAnswer(ignored -> mock(FileIO.class));
        CatalogContext context = CatalogContext.create(new Options(), loader, null);
        FileIO first = new RESTTokenFileIO(context, api, identifier, root).fileIO();
        FileIO disabled = new RESTTokenFileIO(context, api, identifier, root).fileIO();
        FileIO otherEndpoint = new RESTTokenFileIO(context, api, identifier, root).fileIO();
        assertThat(first).isInstanceOf(CachePolicyFileIO.class);
        assertThat(disabled).isNotSameAs(first).isNotInstanceOf(CachePolicyFileIO.class);
        assertThat(otherEndpoint).isNotSameAs(first).isInstanceOf(CachePolicyFileIO.class);
        assertThat(
                        ((HadoopOptionsProvider) first)
                                .hadoopOptions(data, "read")
                                .get("fs.oss.endpoint"))
                .isEqualTo("cache");
        assertThat(
                        ((HadoopOptionsProvider) otherEndpoint)
                                .hadoopOptions(data, "read")
                                .get("fs.oss.endpoint"))
                .isEqualTo("other-cache");
    }

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
    void testIterativeListingsReachInnerOverride() throws IOException {
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
        when(delegate.listStatusIterative(tableRoot)).thenReturn(iterator);
        assertThat(fileIO.listStatusIterative(tableRoot)).isSameAs(iterator);
        // the interface default would construct its own iterator backed by listStatus
        verify(delegate, never()).listStatus(any());
    }
}
