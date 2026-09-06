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

package org.apache.paimon.compact;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileWriterAbortExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a {@link CompactTask} whose result is thrown away does not leave the files it has
 * already written behind.
 */
public class CompactTaskCancelTest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private ExecutorService executor;

    @BeforeEach
    public void before() {
        fileIO = LocalFileIO.create();
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void after() {
        executor.shutdownNow();
        // a test may leave the flag set on the main thread
        Thread.interrupted();
    }

    /**
     * A section that has been rewritten has closed its writer, so its files can only be reached
     * through the result of the whole task. When a later section fails that result is never
     * returned, and the earlier files used to be leaked.
     */
    @Test
    public void testFilesOfFinishedPartsAreDeletedWhenALaterPartFails() throws Exception {
        Path first = writeFile("first");
        Path second = writeFile("second");

        CompactTask task =
                new CompactTask(null, "") {
                    @Override
                    protected CompactResult doCompact() {
                        CompactResult result = new CompactResult();
                        for (Path finished : new Path[] {first, second}) {
                            CompactResult part = finishPart(finished);
                            trackNewFiles(part);
                            result.merge(part);
                        }
                        throw new RuntimeException("rewriting the third section failed");
                    }
                };

        assertThatThrownBy(task::call).hasMessageContaining("third section");

        assertThat(fileIO.exists(first)).isFalse();
        assertThat(fileIO.exists(second)).isFalse();
    }

    /**
     * The interrupt of a cancellation can arrive after the task has written everything. The future
     * then drops the result, so the task itself has to clean up.
     */
    @Test
    public void testFilesAreDeletedWhenCancelledAfterWriting() throws Exception {
        Path written = writeFile("written");

        CompactTask task =
                new CompactTask(null, "") {
                    @Override
                    protected CompactResult doCompact() {
                        CompactResult result = new CompactResult();
                        result.merge(finishPart(written));
                        // the cancellation lands here, once all the files are on disk
                        Thread.currentThread().interrupt();
                        return result;
                    }
                };

        assertThatThrownBy(task::call).isInstanceOf(InterruptedException.class);

        assertThat(fileIO.exists(written)).isFalse();
    }

    /** A task which completes normally keeps its files - they are about to be committed. */
    @Test
    public void testFilesAreKeptWhenTaskSucceeds() throws Exception {
        Path written = writeFile("written");

        CompactTask task =
                new CompactTask(null, "") {
                    @Override
                    protected CompactResult doCompact() {
                        CompactResult result = new CompactResult();
                        result.merge(finishPart(written));
                        return result;
                    }
                };

        assertThat(task.call()).isNotNull();
        assertThat(fileIO.exists(written)).isTrue();
    }

    /**
     * Covers the case the {@code cancelCompaction} TODO described: the task is done writing but
     * still busy, so the interrupt never reaches a point where the task can react to it. Only the
     * manager can clean up then.
     */
    @Test
    @Timeout(30)
    public void testCancelCompactionDeletesFilesOfAnUnresponsiveTask() throws Exception {
        Path written = writeFile("written");

        CountDownLatch tracked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        CompactTask task =
                new CompactTask(null, "") {
                    @Override
                    protected CompactResult doCompact() {
                        CompactResult result = new CompactResult();
                        CompactResult part = finishPart(written);
                        trackNewFiles(part);
                        result.merge(part);
                        tracked.countDown();
                        // busy with work that does not observe interrupts
                        boolean released = false;
                        while (!released) {
                            try {
                                released = release.await(1, TimeUnit.SECONDS);
                            } catch (InterruptedException ignored) {
                                // deliberately swallowed
                            }
                        }
                        return result;
                    }
                };

        TestCompactManager manager = new TestCompactManager();
        manager.submit(executor, task);
        assertThat(tracked.await(30, TimeUnit.SECONDS)).isTrue();

        manager.cancelCompaction();

        assertThat(fileIO.exists(written)).isFalse();
        release.countDown();
    }

    /** Builds the result of one finished part of a task, and hands its files to the task. */
    private CompactResult finishPart(Path file) {
        CompactResult part = new CompactResult();
        part.addAbortExecutors(
                Collections.singletonList(new FileWriterAbortExecutor(fileIO, file)));
        return part;
    }

    private Path writeFile(String name) throws IOException {
        Path path = new Path(tempDir.toUri().toString(), name);
        fileIO.tryToWriteAtomic(path, "some compacted data");
        assertThat(fileIO.exists(path)).isTrue();
        return path;
    }

    private static class TestCompactManager extends CompactFutureManager {

        void submit(ExecutorService executor, CompactTask task) {
            submitTask(executor, task);
        }

        @Override
        public boolean shouldWaitForLatestCompaction() {
            return false;
        }

        @Override
        public boolean shouldWaitForPreparingCheckpoint() {
            return false;
        }

        @Override
        public void addNewFile(DataFileMeta file) {}

        @Override
        public Collection<DataFileMeta> allFiles() {
            return Collections.emptyList();
        }

        @Override
        public void triggerCompaction(boolean fullCompaction) {}

        @Override
        public Optional<CompactResult> getCompactionResult(boolean blocking)
                throws ExecutionException, InterruptedException {
            return innerGetCompactionResult(blocking);
        }

        @Override
        public void close() {}
    }
}
