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

package org.apache.paimon.s3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests cache recovery after Apache HTTP pool shutdown without contacting a real S3 endpoint. */
class S3FileIOCacheRecoveryTest {

    private static final String BUCKET = "paimon-cache-recovery-bucket";

    @BeforeEach
    @AfterEach
    void clearCache() {
        S3FileIO.clearCache();
    }

    @Test
    void testPoolShutdownEvictsAndReplacesFileSystem() throws Exception {
        S3FileIO fileIO = newFileIO("opts-replace");
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem first = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        first.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        assertThat(first.isUsable()).isFalse();

        RecoverableS3AFileSystem second = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        assertThat(second).isNotSameAs(first);
        assertThat(second.isUsable()).isTrue();
        assertThat(fileIO.getFileSystem(path)).isSameAs(second);
    }

    @Test
    void testLateCallbackFromOldInstanceDoesNotDropNewInstance() throws Exception {
        S3FileIO fileIO = newFileIO("opts-late");
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem oldFs = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        oldFs.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        RecoverableS3AFileSystem newFs = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        assertThat(newFs).isNotSameAs(oldFs);

        oldFs.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        assertThat(newFs.isUsable()).isTrue();
        assertThat(fileIO.getFileSystem(path)).isSameAs(newFs);
    }

    @Test
    void testUnrelatedFailureDoesNotEvict() throws Exception {
        S3FileIO fileIO = newFileIO("opts-unrelated");
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem fs = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        fs.handleExecutionFailure(new IOException("timeout"));
        fs.handleExecutionFailure(new IllegalStateException("other illegal state"));
        fs.handleExecutionFailure(
                new IllegalStateException("Connection pool shut down while requesting"));

        assertThat(fs.isUsable()).isTrue();
        assertThat(fileIO.getFileSystem(path)).isSameAs(fs);
    }

    @Test
    void testCauseChainPoolShutdownIsDetected() {
        Throwable nested =
                new IOException("wrap", new IllegalStateException("Connection pool shut down"));
        assertThat(RecoverableS3AFileSystem.isApachePoolShutdown(nested)).isTrue();
        assertThat(RecoverableS3AFileSystem.isApachePoolShutdown(new IOException("x"))).isFalse();

        IOException first = new IOException("first");
        IOException second = new IOException("second");
        first.initCause(second);
        second.initCause(first);
        assertThat(RecoverableS3AFileSystem.isApachePoolShutdown(first)).isFalse();
    }

    @Test
    void testInitIOExceptionClosesUnpublishedAndPropagates() throws Exception {
        IOException failure = new IOException("forced initialize failure");
        verifyInitFailure(failure, failure, "opts-io-fail");
    }

    @Test
    void testInitRuntimeExceptionClosesUnpublishedAndPropagates() throws Exception {
        RuntimeException failure = new IllegalStateException("runtime initialize failure");
        verifyInitFailure(failure, failure, "opts-runtime-fail");
    }

    @Test
    void testInitErrorClosesUnpublishedAndPropagates() throws Exception {
        Error failure = new AssertionError("error initialize failure");
        verifyInitFailure(failure, failure, "opts-error-fail");
    }

    @Test
    void testInitFailureDoesNotAffectDifferentCacheKey() throws Exception {
        Path path = path("s3a", "warehouse/a");
        S3FileIO publishedIO = newFileIO("opts-published");
        RecoverableS3AFileSystem published =
                (RecoverableS3AFileSystem) publishedIO.getFileSystem(path);
        assertThat(published.isUsable()).isTrue();

        AtomicReference<CloseTrackingFileSystem> created = new AtomicReference<>();
        S3FileIO failingIO =
                newFileIO(
                        "opts-init-identity",
                        () ->
                                new CloseTrackingFileSystem() {
                                    @Override
                                    public void initialize(URI uri, Configuration conf)
                                            throws IOException {
                                        created.set(this);
                                        throw new IOException("forced initialize failure");
                                    }
                                });

        // Keep an unusable expectedOld in the failing CacheKey so discardFailedCreate has a target.
        S3FileIO stickyIO =
                newFileIO(
                        "opts-init-identity",
                        () ->
                                new CloseTrackingFileSystem() {
                                    @Override
                                    void bind(Runnable evictFromCache) {
                                        // Keep the unusable instance in CACHE as expectedOld.
                                    }
                                });
        RecoverableS3AFileSystem oldFs = (RecoverableS3AFileSystem) stickyIO.getFileSystem(path);
        oldFs.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));
        assertThat(oldFs.isUsable()).isFalse();

        assertThatThrownBy(() -> failingIO.getFileSystem(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("forced initialize failure");
        assertThat(created.get()).isNotNull();
        assertThat(created.get().closed).isTrue();

        // Other CacheKey's published FS must remain usable.
        assertThat(publishedIO.getFileSystem(path)).isSameAs(published);
        assertThat(published.isUsable()).isTrue();

        RecoverableS3AFileSystem recovered =
                (RecoverableS3AFileSystem) newFileIO("opts-init-identity").getFileSystem(path);
        assertThat(recovered.isUsable()).isTrue();
        assertThat(recovered).isNotSameAs(oldFs);
    }

    @Test
    void testUnusableDuringInitializeIsNotPublishedAndIsClosed() throws Exception {
        Path path = path("s3a", "warehouse/a");
        AtomicReference<CloseTrackingFileSystem> created = new AtomicReference<>();
        S3FileIO fileIO =
                newFileIO(
                        "opts-unusable-during-init",
                        () ->
                                new CloseTrackingFileSystem() {
                                    @Override
                                    public void initialize(URI uri, Configuration conf)
                                            throws IOException {
                                        created.set(this);
                                        super.initialize(uri, conf);
                                        markUnusableAndEvict();
                                    }
                                });

        assertThatThrownBy(() -> fileIO.getFileSystem(path))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("became unusable during initialize");
        assertThat(created.get()).isNotNull();
        assertThat(created.get().closed).isTrue();
        assertThat(created.get().isUsable()).isFalse();

        RecoverableS3AFileSystem recovered =
                (RecoverableS3AFileSystem)
                        newFileIO("opts-unusable-during-init").getFileSystem(path);
        assertThat(recovered.isUsable()).isTrue();
        assertThat(recovered).isNotSameAs(created.get());
    }

    @Test
    void testConcurrentRecoveryCreatesSingleUsableInstance() throws Exception {
        S3FileIO fileIO = newFileIO("opts-concurrent");
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem first = (RecoverableS3AFileSystem) fileIO.getFileSystem(path);
        first.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Future<FileSystem>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    barrier.await();
                                    return fileIO.getFileSystem(path);
                                }));
            }
            FileSystem expected = futures.get(0).get(30, TimeUnit.SECONDS);
            for (Future<FileSystem> future : futures) {
                assertThat(future.get(30, TimeUnit.SECONDS)).isSameAs(expected);
            }
            assertThat(((RecoverableS3AFileSystem) expected).isUsable()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testTwoFileIOInstancesConcurrentlyRecoverToSameFileSystem() throws Exception {
        Options options = baseOptions("opts-two-io-concurrent");
        S3FileIO firstIO = configure(options);
        S3FileIO secondIO = configure(options);
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem oldFs = (RecoverableS3AFileSystem) firstIO.getFileSystem(path);
        assertThat(secondIO.getFileSystem(path)).isSameAs(oldFs);
        oldFs.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<FileSystem> first =
                    executor.submit(
                            () -> {
                                barrier.await();
                                return firstIO.getFileSystem(path);
                            });
            Future<FileSystem> second =
                    executor.submit(
                            () -> {
                                barrier.await();
                                return secondIO.getFileSystem(path);
                            });

            FileSystem recovered = first.get(30, TimeUnit.SECONDS);
            assertThat(second.get(30, TimeUnit.SECONDS)).isSameAs(recovered);
            assertThat(recovered).isNotSameAs(oldFs);
            assertThat(((RecoverableS3AFileSystem) recovered).isUsable()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testTwoFileIOInstancesShareRecoveredCache() throws Exception {
        Options options = baseOptions("opts-shared");
        S3FileIO firstIO = configure(options);
        S3FileIO secondIO = configure(options);
        Path path = path("s3a", "warehouse/a");

        RecoverableS3AFileSystem first = (RecoverableS3AFileSystem) firstIO.getFileSystem(path);
        first.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        RecoverableS3AFileSystem fromSecond =
                (RecoverableS3AFileSystem) secondIO.getFileSystem(path);
        RecoverableS3AFileSystem fromFirst = (RecoverableS3AFileSystem) firstIO.getFileSystem(path);

        assertThat(fromSecond).isSameAs(fromFirst);
        assertThat(fromSecond).isNotSameAs(first);
        assertThat(fromSecond.isUsable()).isTrue();
    }

    private static void verifyInitFailure(
            Throwable toThrow, Throwable expected, String optionMarker) throws Exception {
        Path path = path("s3a", "warehouse/a");
        AtomicReference<CloseTrackingFileSystem> created = new AtomicReference<>();

        // Leave an unusable expectedOld in CACHE so discardFailedCreate exercises identity remove.
        S3FileIO stickyIO =
                newFileIO(
                        optionMarker,
                        () ->
                                new CloseTrackingFileSystem() {
                                    @Override
                                    void bind(Runnable evictFromCache) {
                                        // Keep unusable instance mapped for expectedOld.
                                    }
                                });
        RecoverableS3AFileSystem oldFs = (RecoverableS3AFileSystem) stickyIO.getFileSystem(path);
        oldFs.handleExecutionFailure(new IllegalStateException("Connection pool shut down"));

        S3FileIO failingIO =
                newFileIO(
                        optionMarker,
                        () ->
                                new CloseTrackingFileSystem() {
                                    @Override
                                    public void initialize(URI uri, Configuration conf)
                                            throws IOException {
                                        created.set(this);
                                        if (toThrow instanceof IOException) {
                                            throw (IOException) toThrow;
                                        }
                                        if (toThrow instanceof RuntimeException) {
                                            throw (RuntimeException) toThrow;
                                        }
                                        throw (Error) toThrow;
                                    }
                                });

        assertThatThrownBy(() -> failingIO.getFileSystem(path)).isSameAs(expected);
        assertThat(created.get()).isNotNull();
        assertThat(created.get().closed).isTrue();

        RecoverableS3AFileSystem recovered =
                (RecoverableS3AFileSystem) newFileIO(optionMarker).getFileSystem(path);
        assertThat(recovered.isUsable()).isTrue();
        assertThat(recovered).isNotSameAs(oldFs);
        assertThat(recovered).isNotSameAs(created.get());
    }

    private static S3FileIO newFileIO(String optionMarker) {
        return configure(baseOptions(optionMarker));
    }

    private static S3FileIO newFileIO(
            String optionMarker, Supplier<RecoverableS3AFileSystem> factory) {
        S3FileIO fileIO =
                new S3FileIO() {
                    @Override
                    RecoverableS3AFileSystem newRecoverableFileSystem() {
                        return factory.get();
                    }
                };
        fileIO.configure(CatalogContext.create(baseOptions(optionMarker)));
        return fileIO;
    }

    private static S3FileIO configure(Options options) {
        S3FileIO fileIO = new S3FileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }

    private static Options baseOptions(String optionMarker) {
        Options options = new Options();
        options.set("s3.bucket.probe", "0");
        options.set("s3.endpoint", "http://localhost:1");
        options.set("s3.endpoint.region", "us-east-1");
        options.set("s3.access-key", "dummy-access-key-" + optionMarker);
        options.set("s3.secret-key", "dummy-secret-key");
        options.set("fs.s3a.user.agent.prefix", optionMarker);
        return options;
    }

    private static Path path(String scheme, String key) {
        return new Path(scheme + "://" + BUCKET + "/" + key);
    }

    private static class CloseTrackingFileSystem extends RecoverableS3AFileSystem {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }
}
