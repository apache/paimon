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

package org.apache.paimon.fs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.cache.CachingFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.utils.FileType;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Contract tests for forwarding strict batch-delete capabilities through FileIO wrappers. */
class FileIOBatchDeleteForwardingTest {

    private static final Path FIRST = new Path("oss://bucket/table/a.parquet");
    private static final Path SECOND = new Path("oss://bucket/table/b.parquet");
    private static final List<Path> FILES = Arrays.asList(FIRST, SECOND);

    @Test
    void testPluginForwardsSupportedCapabilityUnderPluginClassLoader() throws Exception {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        BatchDeleteResult expected = result(FILES);
        BatchFileDeleter inner =
                new BatchFileDeleter() {
                    @Override
                    public int maxBatchSize() {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isSameAs(pluginClassLoader);
                        return 1000;
                    }

                    @Override
                    public BatchDeleteResult delete(List<Path> files) {
                        assertThat(Thread.currentThread().getContextClassLoader())
                                .isSameAs(pluginClassLoader);
                        assertThat(files).containsExactlyElementsOf(FILES);
                        return expected;
                    }
                };
        when(delegate.batchFileDeleter(FIRST))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(pluginClassLoader);
                            return Optional.of(inner);
                        });
        TestPluginFileIO plugin = new TestPluginFileIO(delegate, pluginClassLoader);

        // A broken TCCL restore can poison later ServiceLoader tests in the same worker, so the
        // fixture restores the caller loader independently of the production finally block.
        try {
            BatchFileDeleter forwarded =
                    plugin.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

            assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
            assertThat(forwarded.delete(FILES)).isSameAs(expected);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(original);
            verify(delegate).batchFileDeleter(FIRST);
            verify(delegate, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void testPluginForwardsUnsupportedCapability() throws Exception {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        TestPluginFileIO plugin = new TestPluginFileIO(delegate, pluginClassLoader);
        when(delegate.batchFileDeleter(FIRST)).thenReturn(Optional.empty());
        ClassLoader previous = Thread.currentThread().getContextClassLoader();

        try {
            assertThat(plugin.batchFileDeleter(FIRST)).isEmpty();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(previous);
            verify(delegate, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testPluginPropagatesDiscoveryFailureAndRestoresCallerClassLoader() throws Exception {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader callerClassLoader = new ClassLoader(previous) {};
        IOException failure = new IOException("plugin discovery failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(pluginClassLoader);
                            throw failure;
                        });
        TestPluginFileIO plugin = new TestPluginFileIO(delegate, pluginClassLoader);

        Thread.currentThread().setContextClassLoader(callerClassLoader);
        try {
            assertThatThrownBy(() -> plugin.batchFileDeleter(FIRST)).isSameAs(failure);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
            verify(delegate, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testPluginPropagatesDeleteFailureAndRestoresCallerClassLoader() throws Exception {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader callerClassLoader = new ClassLoader(previous) {};
        IOException failure = new IOException("plugin batch failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                new BatchFileDeleter() {
                                    @Override
                                    public int maxBatchSize() {
                                        assertThat(Thread.currentThread().getContextClassLoader())
                                                .isSameAs(pluginClassLoader);
                                        return 1000;
                                    }

                                    @Override
                                    public BatchDeleteResult delete(List<Path> files)
                                            throws IOException {
                                        assertThat(Thread.currentThread().getContextClassLoader())
                                                .isSameAs(pluginClassLoader);
                                        throw failure;
                                    }
                                }));
        TestPluginFileIO plugin = new TestPluginFileIO(delegate, pluginClassLoader);

        Thread.currentThread().setContextClassLoader(callerClassLoader);
        try {
            BatchFileDeleter forwarded =
                    plugin.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
            assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
            assertThatThrownBy(() -> forwarded.delete(FILES)).isSameAs(failure);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
            verify(delegate, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testPluginPropagatesMaxBatchSizeFailureAndRestoresCallerClassLoader() throws Exception {
        FileIO delegate = mock(FileIO.class);
        ClassLoader pluginClassLoader = new ClassLoader() {};
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader callerClassLoader = new ClassLoader(previous) {};
        RuntimeException failure = new RuntimeException("plugin max batch size failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                new BatchFileDeleter() {
                                    @Override
                                    public int maxBatchSize() {
                                        assertThat(Thread.currentThread().getContextClassLoader())
                                                .isSameAs(pluginClassLoader);
                                        throw failure;
                                    }

                                    @Override
                                    public BatchDeleteResult delete(List<Path> files) {
                                        throw new AssertionError("delete must not be called");
                                    }
                                }));
        TestPluginFileIO plugin = new TestPluginFileIO(delegate, pluginClassLoader);

        Thread.currentThread().setContextClassLoader(callerClassLoader);
        try {
            BatchFileDeleter forwarded =
                    plugin.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
            assertThatThrownBy(forwarded::maxBatchSize).isSameAs(failure);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
            verify(delegate, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void testResolvingForwardsSupportedAndUnsupportedCapabilities() throws Exception {
        FileIO supportedDelegate = mock(FileIO.class);
        BatchDeleteResult expected = result(FILES);
        BatchFileDeleter inner = deleter(1000, files -> expected);
        when(supportedDelegate.batchFileDeleter(FIRST)).thenReturn(Optional.of(inner));
        ResolvingFileIO supported = resolving(supportedDelegate);

        BatchFileDeleter forwarded =
                supported.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
        assertThat(forwarded.delete(FILES)).isSameAs(expected);

        FileIO unsupportedDelegate = mock(FileIO.class);
        when(unsupportedDelegate.batchFileDeleter(FIRST)).thenReturn(Optional.empty());
        assertThat(resolving(unsupportedDelegate).batchFileDeleter(FIRST)).isEmpty();
    }

    @Test
    void testResolvingRejectsMixedAuthorityBeforeProviderInvocation() throws Exception {
        FileIO delegate = mock(FileIO.class);
        AtomicInteger providerCalls = new AtomicInteger();
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            providerCalls.incrementAndGet();
                                            return result(files);
                                        })));
        BatchFileDeleter forwarded =
                resolving(delegate).batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        assertThatThrownBy(
                        () ->
                                forwarded.delete(
                                        Arrays.asList(
                                                FIRST,
                                                new Path("oss://other-bucket/table/b.parquet"))))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);
        assertThat(providerCalls).hasValue(0);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testResolvingRejectsMixedSchemeBeforeProviderInvocation() throws Exception {
        FileIO delegate = mock(FileIO.class);
        AtomicInteger providerCalls = new AtomicInteger();
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            providerCalls.incrementAndGet();
                                            return result(files);
                                        })));
        BatchFileDeleter forwarded =
                resolving(delegate).batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        assertThatThrownBy(
                        () ->
                                forwarded.delete(
                                        Arrays.asList(
                                                FIRST, new Path("s3://bucket/table/b.parquet"))))
                .isInstanceOfAny(IllegalArgumentException.class, IOException.class);
        assertThat(providerCalls).hasValue(0);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testResolvingPropagatesProviderFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("resolved batch failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(Optional.of(deleter(1000, files -> raise(failure))));
        BatchFileDeleter forwarded =
                resolving(delegate).batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        assertThatThrownBy(() -> forwarded.delete(FILES)).isSameAs(failure);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testResolvingPropagatesDiscoveryFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("resolved discovery failed");
        when(delegate.batchFileDeleter(FIRST)).thenThrow(failure);

        assertThatThrownBy(() -> resolving(delegate).batchFileDeleter(FIRST)).isSameAs(failure);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testCachingForwardsSupportedAndUnsupportedCapabilities() throws Exception {
        FileIO supportedDelegate = mock(FileIO.class);
        BatchDeleteResult expected = result(FILES);
        when(supportedDelegate.batchFileDeleter(FIRST))
                .thenReturn(Optional.of(deleter(1000, files -> expected)));
        CachingFileIO supported = caching(supportedDelegate);

        BatchFileDeleter forwarded =
                supported.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
        assertThat(forwarded.delete(FILES)).isSameAs(expected);
        verify(supportedDelegate, times(1)).batchFileDeleter(FIRST);

        FileIO unsupportedDelegate = mock(FileIO.class);
        when(unsupportedDelegate.batchFileDeleter(FIRST)).thenReturn(Optional.empty());
        assertThat(caching(unsupportedDelegate).batchFileDeleter(FIRST)).isEmpty();
    }

    @Test
    void testCachingPropagatesProviderFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("cached batch failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(Optional.of(deleter(1000, files -> raise(failure))));
        BatchFileDeleter forwarded =
                caching(delegate).batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        assertThatThrownBy(() -> forwarded.delete(FILES)).isSameAs(failure);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testCachingPropagatesDiscoveryFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("cached discovery failed");
        when(delegate.batchFileDeleter(FIRST)).thenThrow(failure);

        assertThatThrownBy(() -> caching(delegate).batchFileDeleter(FIRST)).isSameAs(failure);
        verify(delegate, times(1)).batchFileDeleter(FIRST);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testRestTokenForwardsSupportedAndUnsupportedCapabilities() throws Exception {
        FileIO supportedDelegate = mock(FileIO.class);
        BatchDeleteResult expected = result(FILES);
        when(supportedDelegate.batchFileDeleter(FIRST))
                .thenReturn(Optional.of(deleter(1000, files -> expected)));
        RESTTokenFileIO supported = restFileIO(supportedDelegate);

        BatchFileDeleter forwarded =
                supported.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
        assertThat(forwarded.delete(FILES)).isSameAs(expected);

        FileIO unsupportedDelegate = mock(FileIO.class);
        when(unsupportedDelegate.batchFileDeleter(FIRST)).thenReturn(Optional.empty());
        assertThat(restFileIO(unsupportedDelegate).batchFileDeleter(FIRST)).isEmpty();
    }

    @Test
    void testRestTokenRefreshDoesNotInvokeStaleDeleter() throws Exception {
        FileIO staleDelegate = mock(FileIO.class);
        FileIO currentDelegate = mock(FileIO.class);
        AtomicInteger staleCalls = new AtomicInteger();
        AtomicInteger currentCalls = new AtomicInteger();
        when(staleDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            staleCalls.incrementAndGet();
                                            return result(files);
                                        })));
        BatchDeleteResult expected = result(FILES);
        when(currentDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            currentCalls.incrementAndGet();
                                            return expected;
                                        })));
        FileIOLoader loader =
                loader(staleDelegate, staleDelegate, currentDelegate, currentDelegate);
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier)).thenReturn(token(0L), token(Long.MAX_VALUE));
        RESTTokenFileIO rest =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null), api, identifier, FIRST);

        BatchFileDeleter forwarded = rest.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThat(forwarded.delete(FILES)).isSameAs(expected);

        assertThat(staleCalls).hasValue(0);
        assertThat(currentCalls).hasValue(1);
        verify(api, times(2)).loadTableToken(identifier);
        verify(staleDelegate, never()).delete(any(), anyBoolean());
        verify(currentDelegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testRestTokenCurrentUnsupportedIsHardFailureWithoutUsingStaleCapability()
            throws Exception {
        FileIO staleDelegate = mock(FileIO.class);
        FileIO currentDelegate = mock(FileIO.class);
        AtomicInteger staleCalls = new AtomicInteger();
        when(staleDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            staleCalls.incrementAndGet();
                                            return result(files);
                                        })));
        when(currentDelegate.batchFileDeleter(FIRST)).thenReturn(Optional.empty());
        RestRefreshFixture fixture = refreshingRest(staleDelegate, currentDelegate);

        BatchFileDeleter forwarded =
                fixture.fileIO.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
        assertThatThrownBy(() -> forwarded.delete(FILES)).isInstanceOf(IOException.class);

        assertThat(staleCalls).hasValue(0);
        verify(fixture.api, times(2)).loadTableToken(fixture.identifier);
        verify(staleDelegate, never()).delete(any(), anyBoolean());
        verify(currentDelegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testRestTokenCurrentDiscoveryFailureIsHardFailureWithoutUsingStaleCapability()
            throws Exception {
        FileIO staleDelegate = mock(FileIO.class);
        FileIO currentDelegate = mock(FileIO.class);
        AtomicInteger staleCalls = new AtomicInteger();
        when(staleDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            staleCalls.incrementAndGet();
                                            return result(files);
                                        })));
        IOException failure = new IOException("refreshed capability discovery failed");
        when(currentDelegate.batchFileDeleter(FIRST)).thenThrow(failure);
        RestRefreshFixture fixture = refreshingRest(staleDelegate, currentDelegate);

        BatchFileDeleter forwarded =
                fixture.fileIO.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
        assertThatThrownBy(() -> forwarded.delete(FILES)).isSameAs(failure);

        assertThat(staleCalls).hasValue(0);
        verify(fixture.api, times(2)).loadTableToken(fixture.identifier);
        verify(staleDelegate, never()).delete(any(), anyBoolean());
        verify(currentDelegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testRestTokenMaxBatchSizeIsDiscoverySnapshotButDeleteUsesCurrentCapability()
            throws Exception {
        FileIO staleDelegate = mock(FileIO.class);
        FileIO currentDelegate = mock(FileIO.class);
        AtomicInteger staleCalls = new AtomicInteger();
        AtomicInteger currentCalls = new AtomicInteger();
        when(staleDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            staleCalls.incrementAndGet();
                                            return result(files);
                                        })));
        BatchDeleteResult expected = result(FILES);
        when(currentDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        7,
                                        files -> {
                                            currentCalls.incrementAndGet();
                                            return expected;
                                        })));
        RestRefreshFixture fixture = refreshingRest(staleDelegate, currentDelegate);

        BatchFileDeleter forwarded =
                fixture.fileIO.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        // The scheduler-facing limit is a discovery snapshot. It does not authorize use of the
        // captured deleter: invocation still refreshes and lets the current provider validate.
        assertThat(forwarded.maxBatchSize()).isEqualTo(1000);
        assertThat(forwarded.delete(FILES)).isSameAs(expected);
        assertThat(staleCalls).hasValue(0);
        assertThat(currentCalls).hasValue(1);
    }

    @Test
    void testRestTokenPropagatesProviderFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("REST batch failed");
        when(delegate.batchFileDeleter(FIRST))
                .thenReturn(Optional.of(deleter(1000, files -> raise(failure))));
        BatchFileDeleter forwarded =
                restFileIO(delegate).batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

        assertThatThrownBy(() -> forwarded.delete(FILES)).isSameAs(failure);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testRestTokenPropagatesDiscoveryFailureWithoutFallback() throws Exception {
        FileIO delegate = mock(FileIO.class);
        IOException failure = new IOException("REST discovery failed");
        when(delegate.batchFileDeleter(FIRST)).thenThrow(failure);

        assertThatThrownBy(() -> restFileIO(delegate).batchFileDeleter(FIRST)).isSameAs(failure);
        verify(delegate, never()).delete(any(), anyBoolean());
    }

    @Test
    void testPluginSerializationForcesCapabilityRediscovery() throws Exception {
        FileIO staleDelegate = mock(FileIO.class);
        FileIO currentDelegate = mock(FileIO.class);
        AtomicInteger staleCalls = new AtomicInteger();
        AtomicInteger currentCalls = new AtomicInteger();
        when(staleDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            staleCalls.incrementAndGet();
                                            return result(files);
                                        })));
        BatchDeleteResult expected = result(FILES);
        when(currentDelegate.batchFileDeleter(FIRST))
                .thenReturn(
                        Optional.of(
                                deleter(
                                        1000,
                                        files -> {
                                            currentCalls.incrementAndGet();
                                            return expected;
                                        })));
        SerializablePluginFileIO.reset(staleDelegate);
        try {
            SerializablePluginFileIO original = new SerializablePluginFileIO();
            assertThat(original.batchFileDeleter(FIRST)).isPresent();

            SerializablePluginFileIO restored = InstantiationUtil.clone(original);
            SerializablePluginFileIO.activeDelegate.set(currentDelegate);
            BatchFileDeleter rediscovered =
                    restored.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);

            assertThat(rediscovered.delete(FILES)).isSameAs(expected);
            assertThat(SerializablePluginFileIO.discoveryCalls.get()).hasValue(2);
            assertThat(staleCalls).hasValue(0);
            assertThat(currentCalls).hasValue(1);
        } finally {
            SerializablePluginFileIO.clear();
        }
    }

    @Test
    void testRestCachingResolvingPluginChainPreservesRefreshAndStrictFailure() throws Exception {
        FileIO staleProvider = mock(FileIO.class);
        FileIO currentProvider = mock(FileIO.class);
        when(staleProvider.exists(any())).thenReturn(true);
        when(currentProvider.exists(any())).thenReturn(true);
        AtomicInteger staleCalls = new AtomicInteger();
        AtomicInteger currentCalls = new AtomicInteger();
        ClassLoader stalePluginClassLoader = new ClassLoader() {};
        ClassLoader currentPluginClassLoader = new ClassLoader() {};
        when(staleProvider.batchFileDeleter(FIRST))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(stalePluginClassLoader);
                            return Optional.of(
                                    deleter(
                                            1000,
                                            files -> {
                                                staleCalls.incrementAndGet();
                                                return result(files);
                                            }));
                        });
        BatchDeleteResult expected = result(FILES);
        when(currentProvider.batchFileDeleter(FIRST))
                .thenAnswer(
                        ignored -> {
                            assertThat(Thread.currentThread().getContextClassLoader())
                                    .isSameAs(currentPluginClassLoader);
                            return Optional.of(
                                    deleter(
                                            1000,
                                            files -> {
                                                assertThat(
                                                                Thread.currentThread()
                                                                        .getContextClassLoader())
                                                        .isSameAs(currentPluginClassLoader);
                                                currentCalls.incrementAndGet();
                                                return expected;
                                            }));
                        });
        FileIO staleChain =
                frozenResolving(new TestPluginFileIO(staleProvider, stalePluginClassLoader));
        FileIO currentChain =
                frozenResolving(new TestPluginFileIO(currentProvider, currentPluginClassLoader));
        FileIOLoader outerLoader = loader(staleChain, staleChain, currentChain, currentChain);
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier)).thenReturn(token(0L), token(Long.MAX_VALUE));
        RESTTokenFileIO rest =
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), outerLoader, null),
                        api,
                        identifier,
                        FIRST);
        CachingFileIO chainRoot = caching(rest);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader callerClassLoader = new ClassLoader(previous) {};

        Thread.currentThread().setContextClassLoader(callerClassLoader);
        try {
            BatchFileDeleter chain =
                    chainRoot.batchFileDeleter(FIRST).orElseThrow(AssertionError::new);
            assertThatThrownBy(
                            () ->
                                    chain.delete(
                                            Arrays.asList(
                                                    FIRST,
                                                    new Path(
                                                            "oss://other-bucket/table/b.parquet"))))
                    .isInstanceOfAny(IllegalArgumentException.class, IOException.class);
            assertThat(staleCalls).hasValue(0);
            assertThat(currentCalls).hasValue(0);

            assertThat(chain.delete(FILES)).isSameAs(expected);
            assertThat(staleCalls).hasValue(0);
            assertThat(currentCalls).hasValue(1);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(callerClassLoader);
            verify(staleProvider, never()).delete(any(), anyBoolean());
            verify(currentProvider, never()).delete(any(), anyBoolean());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static ResolvingFileIO resolving(FileIO delegate) throws IOException {
        FileIOLoader loader = loader(delegate, delegate);
        ResolvingFileIO resolving = new ResolvingFileIO();
        resolving.configure(CatalogContext.create(new Options(), loader, null));
        return resolving;
    }

    private static ResolvingFileIO frozenResolving(FileIO delegate) throws IOException {
        FileIOLoader loader = loader(delegate, delegate);
        FrozenResolvingFileIO resolving = new FrozenResolvingFileIO();
        resolving.configure(CatalogContext.create(new Options(), loader, null));
        return resolving;
    }

    private static CachingFileIO caching(FileIO delegate) {
        return new CachingFileIO(
                delegate,
                mock(org.apache.paimon.fs.cache.LocalCacheManager.class),
                EnumSet.of(FileType.DATA));
    }

    private static RESTTokenFileIO restFileIO(FileIO delegate) {
        FileIOLoader loader = loader(delegate, delegate);
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier)).thenReturn(token(Long.MAX_VALUE));
        return new RESTTokenFileIO(
                CatalogContext.create(new Options(), loader, null), api, identifier, FIRST);
    }

    private static RestRefreshFixture refreshingRest(FileIO staleDelegate, FileIO currentDelegate) {
        FileIOLoader loader =
                loader(staleDelegate, staleDelegate, currentDelegate, currentDelegate);
        RESTApi api = mock(RESTApi.class);
        Identifier identifier = Identifier.create("db", "table");
        when(api.loadTableToken(identifier)).thenReturn(token(0L), token(Long.MAX_VALUE));
        return new RestRefreshFixture(
                new RESTTokenFileIO(
                        CatalogContext.create(new Options(), loader, null), api, identifier, FIRST),
                api,
                identifier);
    }

    private static FileIOLoader loader(FileIO first, FileIO... remaining) {
        FileIOLoader loader = mock(FileIOLoader.class);
        when(loader.getScheme()).thenReturn("oss");
        when(loader.load(any())).thenReturn(first, remaining);
        return loader;
    }

    private static GetTableTokenResponse token(long expiresAtMillis) {
        return new GetTableTokenResponse(
                Collections.singletonMap("token", UUID.randomUUID().toString()), expiresAtMillis);
    }

    private static BatchDeleteResult result(List<Path> files) {
        return new BatchDeleteResult(files);
    }

    private static BatchDeleteResult raise(IOException failure) throws IOException {
        throw failure;
    }

    private static BatchFileDeleter deleter(int maxBatchSize, DeleteAction action) {
        return new BatchFileDeleter() {
            @Override
            public int maxBatchSize() {
                return maxBatchSize;
            }

            @Override
            public BatchDeleteResult delete(List<Path> files) throws IOException {
                return action.delete(files);
            }
        };
    }

    @FunctionalInterface
    private interface DeleteAction {
        BatchDeleteResult delete(List<Path> files) throws IOException;
    }

    private static class RestRefreshFixture {

        private final RESTTokenFileIO fileIO;
        private final RESTApi api;
        private final Identifier identifier;

        private RestRefreshFixture(RESTTokenFileIO fileIO, RESTApi api, Identifier identifier) {
            this.fileIO = fileIO;
            this.api = api;
            this.identifier = identifier;
        }
    }

    private static class FrozenResolvingFileIO extends ResolvingFileIO {

        private boolean initialized;

        @Override
        public void configure(CatalogContext context) {
            if (!initialized) {
                super.configure(context);
                initialized = true;
            }
        }
    }

    private static class SerializablePluginFileIO extends PluginFileIO {

        private static final long serialVersionUID = 1L;

        private static final ThreadLocal<FileIO> activeDelegate = new ThreadLocal<>();
        private static final ThreadLocal<AtomicInteger> discoveryCalls = new ThreadLocal<>();

        private static void reset(FileIO delegate) {
            activeDelegate.set(delegate);
            discoveryCalls.set(new AtomicInteger());
        }

        private static void clear() {
            activeDelegate.remove();
            discoveryCalls.remove();
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            discoveryCalls.get().incrementAndGet();
            return activeDelegate.get();
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return SerializablePluginFileIO.class.getClassLoader();
        }
    }

    private static class TestPluginFileIO extends PluginFileIO {

        private final FileIO delegate;
        private final ClassLoader classLoader;

        private TestPluginFileIO(FileIO delegate, ClassLoader classLoader) {
            this.delegate = delegate;
            this.classLoader = classLoader;
        }

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        protected FileIO createFileIO(Path path) {
            return delegate;
        }

        @Override
        protected ClassLoader pluginClassLoader() {
            return classLoader;
        }
    }
}
