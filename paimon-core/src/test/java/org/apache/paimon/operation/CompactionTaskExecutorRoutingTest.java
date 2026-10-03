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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Focused behavioral tests for {@code compaction.task-threads} executor routing. */
class CompactionTaskExecutorRoutingTest {

    private ExecutorService externalExecutor;

    @AfterEach
    void tearDown() {
        if (externalExecutor != null) {
            externalExecutor.shutdownNow();
            externalExecutor = null;
        }
    }

    @Test
    void testFixedPoolUsesSharedThreadPoolWithConfiguredSize() throws Exception {
        ExecutorRoutingWrite write = new ExecutorRoutingWrite(coreOptions(2));
        ExecutorService bucket0 = write.compactExecutorForTesting(EMPTY_ROW, 0);
        ExecutorService bucket1 = write.compactExecutorForTesting(EMPTY_ROW, 1);
        assertThat(bucket0).isSameAs(bucket1);
        assertThat(bucket0).isInstanceOf(ThreadPoolExecutor.class);
        assertThat(((ThreadPoolExecutor) bucket0).getMaximumPoolSize()).isEqualTo(2);
        write.close();
    }

    @Test
    void testFixedPoolAllowsCrossBucketParallelism() throws Exception {
        ExecutorRoutingWrite write = new ExecutorRoutingWrite(coreOptions(2));
        ExecutorService pool = write.compactExecutorForTesting(EMPTY_ROW, 0);

        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);

        Future<?> first =
                pool.submit(
                        () -> {
                            firstStarted.countDown();
                            try {
                                releaseFirst.await(30, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
        Future<?> second =
                pool.submit(
                        () -> {
                            secondStarted.countDown();
                        });

        assertThat(firstStarted.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(secondStarted.await(30, TimeUnit.SECONDS)).isTrue();

        releaseFirst.countDown();
        first.get(30, TimeUnit.SECONDS);
        second.get(30, TimeUnit.SECONDS);
        write.close();
    }

    @Test
    void testPerBucketUsesDedicatedExecutorsAndReusesSameBucketExecutor() throws Exception {
        BinaryRow partition = EMPTY_ROW.copy();
        ExecutorRoutingWrite write = new ExecutorRoutingWrite(coreOptions(-1));

        ExecutorService bucket0 = write.compactExecutorForTesting(partition, 0);
        ExecutorService bucket1 = write.compactExecutorForTesting(partition, 1);
        assertThat(bucket0).isNotSameAs(bucket1);
        assertThat(write.activePerBucketExecutorCountForTesting()).isEqualTo(2);

        ExecutorService bucket0Again = write.compactExecutorForTesting(partition, 0);
        assertThat(bucket0Again).isSameAs(bucket0);
        write.close();
    }

    @Test
    void testPerBucketReleaseShutsDownAndRecreatesExecutor() throws Exception {
        BinaryRow partition = EMPTY_ROW.copy();
        ExecutorRoutingWrite write = new ExecutorRoutingWrite(coreOptions(-1));
        ExecutorService first = write.compactExecutorForTesting(partition, 0);
        assertThat(write.activePerBucketExecutorCountForTesting()).isEqualTo(1);

        write.releaseCompactionExecutorForTesting(partition, 0);
        assertThat(write.activePerBucketExecutorCountForTesting()).isZero();
        assertThat(first.isShutdown()).isTrue();

        ExecutorService second = write.compactExecutorForTesting(partition, 0);
        assertThat(second).isNotSameAs(first);
        write.close();
    }

    @Test
    void testExternalExecutorIsSharedAndNotClosedByWrite() throws Exception {
        externalExecutor = Executors.newSingleThreadExecutor();
        ExecutorRoutingWrite write = new ExecutorRoutingWrite(coreOptions(-1));
        write.withCompactExecutor(externalExecutor);

        ExecutorService bucket0 = write.compactExecutorForTesting(EMPTY_ROW, 0);
        ExecutorService bucket1 = write.compactExecutorForTesting(EMPTY_ROW, 1);
        assertThat(bucket0).isSameAs(externalExecutor);
        assertThat(bucket1).isSameAs(externalExecutor);
        assertThat(write.activePerBucketExecutorCountForTesting()).isZero();

        write.close();
        assertThat(externalExecutor.isShutdown()).isFalse();
    }

    private static CoreOptions coreOptions(int compactionTaskThreads) {
        Options options = new Options();
        options.set(CoreOptions.COMPACTION_TASK_THREADS, compactionTaskThreads);
        return new CoreOptions(options);
    }

    private static class ExecutorRoutingWrite extends AbstractFileStoreWrite<String> {

        private ExecutorRoutingWrite(CoreOptions coreOptions) {
            super(
                    mock(SnapshotManager.class),
                    mock(FileStoreScan.class),
                    null,
                    null,
                    null,
                    "test-table",
                    coreOptions,
                    RowType.of());
        }

        @Override
        protected Function<WriterContainer<String>, Boolean> createWriterCleanChecker() {
            return writer -> false;
        }

        @Override
        protected RecordWriter<String> createWriter(
                BinaryRow partition,
                int bucket,
                List<DataFileMeta> restoreFiles,
                long restoredMaxSeqNumber,
                @Nullable CommitIncrement restoreIncrement,
                ExecutorService compactExecutor,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
                boolean ignorePreviousFiles) {
            return mock(RecordWriter.class);
        }
    }
}
