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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.io.cache.Cache;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.ThrowingConsumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests that {@link BTreeIndexReader} always releases the file handle it opens. */
public class BTreeIndexReaderCloseTest {

    private static final int RECORD_NUM = 1000;

    @TempDir private java.nio.file.Path tempPath;

    private FileIO fileIO;
    private KeySerializer keySerializer;
    private GlobalIndexIOMeta meta;

    @BeforeEach
    public void setUp() throws Exception {
        fileIO = LocalFileIO.create();
        IntType dataType = new IntType();
        keySerializer = KeySerializer.create(dataType);

        GlobalIndexFileWriter fileWriter =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return "test-btree-" + UUID.randomUUID() + prefix;
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String fileName)
                            throws IOException {
                        return fileIO.newOutputStream(path(fileName), true);
                    }
                };

        BTreeGlobalIndexer indexer =
                new BTreeGlobalIndexer(new DataField(1, "testField", dataType), new Options());
        GlobalIndexSingleColumnWriter writer = indexer.createWriter(fileWriter);
        for (int i = 0; i < RECORD_NUM; i++) {
            writer.write(i, (long) i);
        }
        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        ResultEntry entry = results.get(0);
        Path filePath = path(entry.fileName());
        meta = new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), results.get(0).meta());
    }

    /** A reader over a healthy file keeps the handle open, and close() releases it. */
    @Test
    public void testCloseReleasesTheInput() throws Exception {
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexReader reader =
                new BTreeIndexReader(
                        keySerializer,
                        tracking(closed),
                        meta,
                        new CacheManager(MemorySize.VALUE_8_MB, 0));
        assertThat(closed).hasValue(0);

        reader.close();
        assertThat(closed).hasValue(1);
    }

    /**
     * A corrupted footer makes the constructor fail after the file handle has been opened. Nothing
     * else holds a reference to it at that point, so the constructor has to release it itself.
     */
    @Test
    public void testFailedConstructionReleasesTheInput() throws Exception {
        corruptFooterMagic();

        AtomicInteger closed = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                new BTreeIndexReader(
                                        keySerializer,
                                        tracking(closed),
                                        meta,
                                        new CacheManager(MemorySize.VALUE_8_MB, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bad magic number");

        assertThat(closed).hasValue(1);
    }

    /**
     * The reader and the file handle are two separate resources. A reader that fails to close must
     * still surface its own failure unchanged, but it must not strand the handle.
     */
    @Test
    public void testCloseReleasesTheInputWhenTheReaderFails() throws Exception {
        FailingCacheManager cacheManager = new FailingCacheManager();
        AtomicInteger closed = new AtomicInteger();
        BTreeIndexReader reader =
                new BTreeIndexReader(keySerializer, tracking(closed), meta, cacheManager);

        cacheManager.failing = true;
        assertThatThrownBy(reader::close)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("cache is down");

        assertThat(closed).hasValue(1);
    }

    @Test
    public void testClosePreventsQueuedQueriesFromOpeningFiles() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch resumeWorker = new CountDownLatch(1);
        executor.submit(() -> await(resumeWorker));
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        CacheManager cacheManager = new CacheManager(MemorySize.VALUE_8_MB, 0);
        LazyFilteredBTreeReader reader =
                new LazyFilteredBTreeReader(
                        Collections.singletonList(meta),
                        keySerializer,
                        ioMeta -> {
                            opened.incrementAndGet();
                            return tracking(closed).getInputStream(ioMeta);
                        },
                        cacheManager,
                        Long.MAX_VALUE,
                        RECORD_NUM,
                        executor);
        try {
            FieldRef ref = new FieldRef(1, "testField", new IntType());
            CompletableFuture<Optional<GlobalIndexResult>> queued = reader.visitEqual(ref, 42);
            reader.close();
            resumeWorker.countDown();

            assertThatThrownBy(() -> queued.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> reader.visitEqual(ref, 42).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(opened).hasValue(0);
            assertThat(closed).hasValue(0);
            assertThat(cacheManager.dataCache().asMap()).isEmpty();
        } finally {
            resumeWorker.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
            cacheManager.close();
        }
    }

    @Test
    public void testCloseAttemptsEveryReaderWhenInputsFailToClose() throws Exception {
        java.nio.file.Path copy = tempPath.resolve("second-btree");
        Files.copy(java.nio.file.Paths.get(meta.filePath().toUri()), copy);
        GlobalIndexIOMeta second =
                new GlobalIndexIOMeta(new Path(copy.toUri()), meta.fileSize(), meta.metadata());
        IOException ioFailure = new IOException("input close failed");
        RuntimeException runtimeFailure = new IllegalStateException("input close failed");
        AtomicInteger closed = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CacheManager cacheManager = new CacheManager(MemorySize.VALUE_8_MB, 0);
        try (LazyFilteredBTreeReader reader =
                new LazyFilteredBTreeReader(
                        Arrays.asList(meta, second),
                        keySerializer,
                        tracking(
                                closed,
                                ioMeta -> {
                                    if (ioMeta.filePath().equals(meta.filePath())) {
                                        throw ioFailure;
                                    }
                                    throw runtimeFailure;
                                }),
                        cacheManager,
                        Long.MAX_VALUE,
                        RECORD_NUM,
                        executor)) {
            reader.visitEqual(new FieldRef(1, "testField", new IntType()), 42)
                    .get(10, TimeUnit.SECONDS);
            Throwable failure = catchThrowable(reader::close);
            assertThat(closed).hasValue(2);
            assertThat(failure).isIn(ioFailure, runtimeFailure);
            assertThat(failure.getSuppressed())
                    .containsExactly(failure == ioFailure ? runtimeFailure : ioFailure);
            assertThat(cacheManager.dataCache().asMap()).isEmpty();

            reader.close();
            assertThat(closed).hasValue(2);
        } finally {
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            cacheManager.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInterruptedQueryClosesInFlightReader(boolean pauseWhileOpening)
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch queryStarted = new CountDownLatch(pauseWhileOpening ? 1 : 2);
        CountDownLatch resumeQuery = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        AtomicInteger completedVisits = new AtomicInteger();
        CacheManager cacheManager = new CacheManager(MemorySize.VALUE_8_MB, 0);
        Runnable pauseQuery =
                () -> {
                    queryStarted.countDown();
                    await(resumeQuery);
                };
        LazyFilteredBTreeReader reader =
                new LazyFilteredBTreeReader(
                        Collections.singletonList(meta),
                        keySerializer,
                        ioMeta -> {
                            SeekableInputStream input = tracking(closed).getInputStream(ioMeta);
                            opened.incrementAndGet();
                            if (pauseWhileOpening) {
                                pauseQuery.run();
                            }
                            return input;
                        },
                        cacheManager,
                        Long.MAX_VALUE,
                        RECORD_NUM,
                        executor) {
                    @Override
                    protected Optional<GlobalIndexResult> visitEqual(
                            BTreeIndexReader reader, Object literal) {
                        if (!pauseWhileOpening) {
                            pauseQuery.run();
                        }
                        Optional<GlobalIndexResult> result = super.visitEqual(reader, literal);
                        completedVisits.incrementAndGet();
                        return result;
                    }

                    @Override
                    public void close() throws IOException {
                        closeStarted.countDown();
                        super.close();
                    }
                };
        RowType rowType =
                new RowType(
                        Collections.singletonList(new DataField(1, "testField", new IntType())));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptedAfterClose = new AtomicBoolean();
        Thread caller =
                new Thread(
                        () -> {
                            try (GlobalIndexEvaluator evaluator =
                                    new GlobalIndexEvaluator(
                                            rowType, id -> Collections.singletonList(reader))) {
                                evaluator.evaluate(new PredicateBuilder(rowType).equal(0, 42));
                            } catch (Throwable e) {
                                failure.set(e);
                            } finally {
                                interruptedAfterClose.set(Thread.currentThread().isInterrupted());
                            }
                        });
        try {
            caller.start();
            // Both visits must enter before either is released: reads must remain concurrent.
            CompletableFuture<Optional<GlobalIndexResult>> concurrentQuery =
                    pauseWhileOpening
                            ? null
                            : reader.visitEqual(new FieldRef(1, "testField", new IntType()), 43);
            assertThat(queryStarted.await(10, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            // Wait until close either waits for the active query or incorrectly returns early.
            CommonTestUtils.waitUtil(
                    () -> caller.getState() == Thread.State.WAITING || !caller.isAlive(),
                    Duration.ofSeconds(10),
                    Duration.ofMillis(1));
            assertThat(caller.isAlive()).isTrue();
            assertThat(closed).hasValue(0);

            resumeQuery.countDown();
            caller.join(10000);
            assertThat(caller.isAlive()).isFalse();
            assertThat(failure.get())
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(interruptedAfterClose).isTrue();
            assertThat(opened).hasValue(1);
            assertThat(closed).hasValue(1);
            assertThat(completedVisits).hasValue(pauseWhileOpening ? 1 : 2);
            assertThat(cacheManager.dataCache().asMap()).isEmpty();
            if (concurrentQuery != null) {
                assertThat(concurrentQuery.get(10, TimeUnit.SECONDS).get().results().iterator())
                        .toIterable()
                        .containsExactly(43L);
            }

            reader.close();
            assertThat(closed).hasValue(1);
        } finally {
            resumeQuery.countDown();
            caller.join(10000);
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
            cacheManager.close();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private Path path(String fileName) {
        return new Path(new Path(tempPath.toUri()), fileName);
    }

    /** Overwrite the four magic-number bytes the footer ends with. */
    private void corruptFooterMagic() throws IOException {
        try (RandomAccessFile file =
                new RandomAccessFile(new java.io.File(meta.filePath().toUri()), "rw")) {
            file.seek(file.length() - 4);
            file.writeInt(~BTreeFileFooter.MAGIC_NUMBER);
        }
    }

    private GlobalIndexFileReader tracking(AtomicInteger closed) {
        return tracking(closed, ioMeta -> {});
    }

    private GlobalIndexFileReader tracking(
            AtomicInteger closed, ThrowingConsumer<GlobalIndexIOMeta, IOException> onClose) {
        return ioMeta -> {
            SeekableInputStream delegate = fileIO.newInputStream(ioMeta.filePath());
            return new SeekableInputStream() {
                @Override
                public void seek(long desired) throws IOException {
                    delegate.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public int read() throws IOException {
                    return delegate.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return delegate.read(b, off, len);
                }

                @Override
                public void close() throws IOException {
                    closed.incrementAndGet();
                    delegate.close();
                    onClose.accept(ioMeta);
                }
            };
        };
    }

    /** Fails page invalidation during reader close. */
    private static class FailingCacheManager extends CacheManager {

        private boolean failing = false;

        FailingCacheManager() {
            super(MemorySize.VALUE_8_MB, 0);
        }

        @Override
        protected void invalidPage(CacheKey key, Cache.CacheValue expected) {
            if (failing) {
                throw new RuntimeException("cache is down");
            }
            super.invalidPage(key, expected);
        }

        @Override
        public void invalidPage(CacheKey key) {
            if (failing) {
                throw new RuntimeException("cache is down");
            }
            super.invalidPage(key);
        }
    }
}
