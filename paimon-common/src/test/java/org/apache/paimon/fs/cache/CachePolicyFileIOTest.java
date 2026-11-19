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

package org.apache.paimon.fs.cache;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ORIGIN_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests policy routing with a FileIO that does not depend on Jindo or OSS SDKs. */
class CachePolicyFileIOTest {

    private static final Path ROOT = new Path("oss://bucket/table");
    private static final Path DATA = new Path(ROOT, "bucket-0/data.orc");
    private static final Path MUTABLE = new Path(ROOT, "snapshot/LATEST");

    @Test
    void testRoutesStreamsAndMetadataAndKeepsMutableFilesOnOrigin() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        try (FileIO fileIO = create(options(), loader)) {
            fileIO.newInputStream(DATA);
            fileIO.newOutputStream(DATA, false);
            fileIO.getFileStatus(DATA);
            fileIO.exists(DATA);
            fileIO.getFileSize(DATA);
            fileIO.isDir(DATA);
            assertThat(loader.delegate("cache").calls)
                    .containsSubsequence("read", "write", "stat", "exists", "stat", "stat");
            RecordingFileIO origin = loader.delegate("origin");
            origin.calls.clear();
            fileIO.newInputStream(MUTABLE);
            fileIO.newOutputStream(MUTABLE, true);
            fileIO.getFileStatus(MUTABLE);
            assertThat(origin.calls).containsExactly("read", "write", "stat");
        }
    }

    @Test
    void testOriginOnlyOperationsPreserveDelegateOverrides() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        try (FileIO fileIO = create(options(), loader)) {
            RecordingFileIO origin = loader.delegate("origin");
            origin.calls.clear();
            int created = loader.created.size();
            fileIO.listStatus(ROOT);
            fileIO.listFilesIterative(ROOT, true);
            fileIO.listDirectories(ROOT);
            fileIO.delete(DATA, false);
            fileIO.mkdirs(ROOT);
            fileIO.rename(DATA, MUTABLE);
            fileIO.tryToWriteAtomic(MUTABLE, "value");
            fileIO.newTwoPhaseOutputStream(DATA, true);
            fileIO.createBlobPresignedUrl(
                    ROOT, new BlobDescriptor(DATA.toString(), 0, 1), Duration.ofMinutes(1));
            fileIO.archive(DATA, null);
            fileIO.restoreArchive(DATA, Duration.ofMinutes(1));
            fileIO.unarchive(DATA, null);
            assertThat(origin.calls)
                    .containsExactly(
                            "list",
                            "iterate",
                            "directories",
                            "delete",
                            "mkdirs",
                            "rename",
                            "atomic",
                            "two-phase",
                            "presign",
                            "archive",
                            "restore",
                            "unarchive");
            assertThat(loader.created).hasSize(created);
        }
    }

    @Test
    void testDisabledPolicyStillAppliesOriginEndpoint() throws Exception {
        for (String policy : new String[] {null, "none"}) {
            Options options = options();
            if (policy == null) {
                options.remove(IO_CACHE_POLICY);
            } else {
                options.set(IO_CACHE_POLICY, policy);
            }
            RecordingLoader loader = new RecordingLoader();
            try (FileIO fileIO = create(options, loader)) {
                assertThat(fileIO).isInstanceOf(RecordingFileIO.class);
                assertThat(
                                ((HadoopOptionsProvider) fileIO)
                                        .hadoopOptions(DATA, "read")
                                        .get("fs.oss.endpoint"))
                        .isEqualTo("origin");
                assertThat(options.get("fs.oss.endpoint")).isEqualTo("cache");
                assertThat(loader.delegate("origin").options.keySet())
                        .doesNotContain(IO_CACHE_ORIGIN_ENDPOINT.key());
            }
        }
    }

    @Test
    void testLegacyJindoCacheSwitchDoesNotControlEndpointRouting() throws Exception {
        for (boolean legacyEnabled : new boolean[] {false, true}) {
            Options options = options();
            options.set(IO_CACHE_ENABLED, legacyEnabled);
            options.set(IO_CACHE_WHITELIST_PATH, "bucket-0");
            options.set("fs.jindocache.namespace.rpc.address", "legacy-cache:1234");
            RecordingLoader loader = new RecordingLoader();
            try (FileIO fileIO = create(options, loader)) {
                fileIO.newInputStream(DATA);
                assertThat(loader.delegate("cache").calls).contains("read");
                for (String endpoint : new String[] {"origin", "cache"}) {
                    Options delegateOptions = loader.delegate(endpoint).options;
                    assertThat(delegateOptions.get(IO_CACHE_ENABLED)).isEqualTo(legacyEnabled);
                    assertThat(delegateOptions.get(IO_CACHE_POLICY)).isEqualTo("meta,read,write");
                    assertThat(delegateOptions.get(IO_CACHE_WHITELIST_PATH)).isEqualTo("bucket-0");
                    assertThat(delegateOptions.get("fs.jindocache.namespace.rpc.address"))
                            .isEqualTo("legacy-cache:1234");
                }
            }
        }
    }

    @Test
    void testNativeOptionsUseTheSamePolicyWithResolvingFileIO() throws Exception {
        Options options = options();
        options.set(RESOLVING_FILE_IO_ENABLED, true);
        options.set(IO_CACHE_POLICY, "read");
        options.set(IO_CACHE_WHITELIST_PATH, "bucket-0");
        RecordingLoader loader = new RecordingLoader();
        try (FileIO fileIO = create(options, loader)) {
            HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
            assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.endpoint"))
                    .isEqualTo("cache");
            assertThat(provider.hadoopOptions(DATA, "write").get("fs.oss.endpoint"))
                    .isEqualTo("origin");
            assertThat(provider.hadoopOptions(MUTABLE, "read").get("fs.oss.endpoint"))
                    .isEqualTo("origin");
            assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.securityToken"))
                    .isEqualTo("test-token");
        }
    }

    @Test
    void testMissingOriginEndpointRetainsCacheEndpoint() throws Exception {
        Options options = options();
        options.remove(IO_CACHE_ORIGIN_ENDPOINT);
        try (FileIO fileIO = create(options, new RecordingLoader())) {
            HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
            assertThat(provider.hadoopOptions(DATA, "write").get("fs.oss.endpoint"))
                    .isEqualTo("cache");
            assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.endpoint"))
                    .isEqualTo("cache");
        }
    }

    @Test
    void testCacheAlwaysUsesStandardOssEndpoint() throws Exception {
        Options options = options();
        options.set("io-cache.endpoint", "unused-cache");
        try (FileIO fileIO = create(options, new RecordingLoader())) {
            HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
            assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.endpoint"))
                    .isEqualTo("cache");
            assertThat(provider.hadoopOptions(MUTABLE, "read").get("fs.oss.endpoint"))
                    .isEqualTo("origin");
            assertThat(options.get("fs.oss.endpoint")).isEqualTo("cache");
        }
    }

    @Test
    void testLazyConcurrentInitializationAndRuntimeContext() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        try (FileIO fileIO = create(options(), loader)) {
            int originCreated = loader.created.size();
            fileIO.setRuntimeContext(Collections.singletonMap("runtime", "before"));
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                List<Callable<Options>> tasks = new ArrayList<>();
                for (int i = 0; i < 16; i++) {
                    tasks.add(() -> ((HadoopOptionsProvider) fileIO).hadoopOptions(DATA, "read"));
                }
                for (Future<Options> result : executor.invokeAll(tasks)) {
                    assertThat(result.get().get("fs.oss.endpoint")).isEqualTo("cache");
                }
            } finally {
                executor.shutdownNow();
            }
            // FileIO.get probes the preferred loader once before loading the actual delegate.
            assertThat(loader.created).hasSize(originCreated + 2);
            assertThat(loader.delegate("cache").runtime).containsEntry("runtime", "before");
            fileIO.setRuntimeContext(Collections.singletonMap("runtime", "after"));
            assertThat(loader.delegate("origin").runtime).containsEntry("runtime", "after");
            assertThat(loader.delegate("cache").runtime).containsEntry("runtime", "after");
        }
    }

    @Test
    void testSerializationRecreatesLazyCacheAndRetainsRuntimeContext() throws Exception {
        try (FileIO original = create(options(), new RecordingLoader())) {
            original.setRuntimeContext(Collections.singletonMap("runtime", "serialized"));
            original.newInputStream(DATA);
            try (FileIO cloned = InstantiationUtil.clone(original)) {
                assertThat(
                                ((HadoopOptionsProvider) cloned)
                                        .hadoopOptions(DATA, "read")
                                        .get("fs.oss.endpoint"))
                        .isEqualTo("cache");
                assertThat(
                                ((HadoopOptionsProvider) cloned)
                                        .hadoopOptions(DATA, "read")
                                        .get("runtime"))
                        .isEqualTo("serialized");
                assertThat(((CachePolicyFileIO) cloned).originFileIO())
                        .isNotSameAs(((CachePolicyFileIO) original).originFileIO());
            }
        }
    }

    @Test
    void testCloseDoesNotCreateCacheAndPreventsReopening() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        FileIO fileIO = create(options(), loader);
        int created = loader.created.size();
        fileIO.close();
        fileIO.close();
        assertThat(loader.created).hasSize(created);
        assertThat(loader.delegate("origin").closeCount).isEqualTo(1);
        assertThatThrownBy(() -> fileIO.newInputStream(DATA))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> fileIO.setRuntimeContext(Collections.emptyMap()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCloseAttemptsBothDelegatesAndPreservesFailures() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        FileIO fileIO = create(options(), loader);
        fileIO.newInputStream(DATA);
        RecordingFileIO origin = loader.delegate("origin");
        RecordingFileIO cache = loader.delegate("cache");
        origin.failClose = true;
        cache.failClose = true;
        assertThatThrownBy(fileIO::close)
                .isInstanceOf(IOException.class)
                .hasMessage("origin")
                .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
        fileIO.close();
        assertThat(origin.closeCount).isEqualTo(1);
        assertThat(cache.closeCount).isEqualTo(1);
    }

    @Test
    void testCacheReadFailuresAreNotRetriedOnOrigin() throws Exception {
        RecordingLoader loader = new RecordingLoader();
        try (FileIO fileIO = create(options(), loader)) {
            fileIO.newInputStream(DATA);
            loader.delegate("cache").failRead = true;
            assertThatThrownBy(() -> fileIO.newInputStream(DATA))
                    .isInstanceOf(IOException.class)
                    .hasMessage("cache read failed");
            assertThat(loader.delegate("origin").calls).doesNotContain("read");
        }
    }

    private static Options options() {
        Options options = new Options();
        options.set(IO_CACHE_POLICY, "meta,read,write");
        options.set(IO_CACHE_ORIGIN_ENDPOINT, "origin");
        options.set("fs.oss.endpoint", "cache");
        options.set("fs.oss.securityToken", "test-token");
        return options;
    }

    private static FileIO create(Options options, RecordingLoader loader) throws IOException {
        return CachePolicyFileIO.create(ROOT, CatalogContext.create(options, loader, null));
    }

    private static class RecordingLoader implements FileIOLoader {

        private final List<RecordingFileIO> created = new ArrayList<>();

        @Override
        public String getScheme() {
            return "oss";
        }

        @Override
        public FileIO load(Path path) {
            RecordingFileIO fileIO = new RecordingFileIO();
            created.add(fileIO);
            return fileIO;
        }

        private RecordingFileIO delegate(String endpoint) {
            for (int i = created.size() - 1; i >= 0; i--) {
                RecordingFileIO fileIO = created.get(i);
                if (endpoint.equals(fileIO.options.get("fs.oss.endpoint"))) {
                    return fileIO;
                }
            }
            throw new AssertionError("No delegate for " + endpoint);
        }
    }

    private static class RecordingFileIO extends LocalFileIO implements HadoopOptionsProvider {

        private Options options;
        private final List<String> calls = new ArrayList<>();
        private final Map<String, String> runtime = new HashMap<>();
        private int closeCount;
        private boolean failClose;
        private boolean failRead;

        @Override
        public void configure(CatalogContext context) {
            options = context.options();
        }

        @Override
        public Options hadoopOptions(Path path, String opType) {
            Options result = new Options(options.toMap());
            runtime.forEach(result::set);
            return result;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            calls.add("read");
            if (failRead) {
                throw new IOException("cache read failed");
            }
            return mock(SeekableInputStream.class);
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite) {
            calls.add("write");
            return mock(PositionOutputStream.class);
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            calls.add("stat");
            return mock(FileStatus.class);
        }

        @Override
        public boolean exists(Path path) {
            calls.add("exists");
            return true;
        }

        @Override
        public FileStatus[] listStatus(Path path) {
            calls.add("list");
            return new FileStatus[0];
        }

        @Override
        @SuppressWarnings("unchecked")
        public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive) {
            calls.add("iterate");
            return mock(RemoteIterator.class);
        }

        @Override
        public FileStatus[] listDirectories(Path path) {
            calls.add("directories");
            return new FileStatus[0];
        }

        @Override
        public boolean delete(Path path, boolean recursive) {
            calls.add("delete");
            return true;
        }

        @Override
        public boolean mkdirs(Path path) {
            calls.add("mkdirs");
            return true;
        }

        @Override
        public boolean rename(Path src, Path dst) {
            calls.add("rename");
            return true;
        }

        @Override
        public boolean tryToWriteAtomic(Path path, String content) {
            calls.add("atomic");
            return true;
        }

        @Override
        public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite) {
            calls.add("two-phase");
            return mock(TwoPhaseOutputStream.class);
        }

        @Override
        public String createBlobPresignedUrl(Path root, BlobDescriptor blob, Duration validity) {
            calls.add("presign");
            return "https://origin";
        }

        @Override
        public Optional<Path> archive(Path path, StorageType type) {
            calls.add("archive");
            return Optional.of(path);
        }

        @Override
        public void restoreArchive(Path path, Duration duration) {
            calls.add("restore");
        }

        @Override
        public Optional<Path> unarchive(Path path, StorageType type) {
            calls.add("unarchive");
            return Optional.of(path);
        }

        @Override
        public void setRuntimeContext(Map<String, String> options) {
            runtime.putAll(options);
        }

        @Override
        public void close() throws IOException {
            closeCount++;
            if (failClose) {
                throw new IOException(options.get("fs.oss.endpoint"));
            }
        }
    }
}
