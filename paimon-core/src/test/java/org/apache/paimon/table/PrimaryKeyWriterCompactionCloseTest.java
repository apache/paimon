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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/** Tests for releasing owned compaction rewriters after asynchronous failure. */
class PrimaryKeyWriterCompactionCloseTest extends TableTestBase {

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testCloseReleasesRewriter(boolean failCompaction, boolean failRewriterClose)
            throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .primaryKey("k")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.WRITE_ONLY.key(), "false")
                        .option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "99")
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        Path resourcePath = new Path(tempPath.resolve("rewriter-resource").toString());
        PositionOutputStream resource = new TraceableFileIO().newOutputStream(resourcePath, false);
        AtomicBoolean closed = new AtomicBoolean();
        AtomicReference<CompactRewriter> created = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
                    @Override
                    protected void afterExecute(Runnable task, Throwable failure) {
                        super.afterExecute(task, failure);
                        completed.countDown();
                    }
                };
        StreamTableWrite write = table.newWrite("close-contract");
        StreamTableCommit commit = table.newCommit("close-contract");
        try {
            ((TableWriteImpl<?>) write).withCompactExecutor(executor);
            write.withCompactRewriterFactory(
                    (partition, bucket, delegate) -> {
                        CompactRewriter rewriter =
                                new CompactRewriter() {
                                    @Override
                                    public CompactResult rewrite(
                                            int level,
                                            boolean dropDelete,
                                            List<List<SortedRun>> sections)
                                            throws Exception {
                                        if (failCompaction) {
                                            throw new IOException("expected compaction failure");
                                        }
                                        return delegate.rewrite(level, dropDelete, sections);
                                    }

                                    @Override
                                    public CompactResult upgrade(int level, DataFileMeta file)
                                            throws Exception {
                                        if (failCompaction) {
                                            throw new IOException("expected compaction failure");
                                        }
                                        return delegate.upgrade(level, file);
                                    }

                                    @Override
                                    public void close() throws IOException {
                                        closed.set(true);
                                        try {
                                            IOUtils.closeAll(delegate, resource);
                                        } catch (Exception e) {
                                            throw new IOException(e);
                                        }
                                        if (failRewriterClose) {
                                            throw new IOException(
                                                    "expected rewriter close failure");
                                        }
                                    }
                                };
                        created.set(rewriter);
                        return rewriter;
                    });
            write.write(GenericRow.of(1, BinaryString.fromString("committed")));
            commit.commit(0, write.prepareCommit(false, 0));
            assertThat(read(table))
                    .extracting(row -> row.getString(1).toString())
                    .containsExactly("committed");
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
            Throwable failure = catchThrowable(write::close);
            List<?> openResources =
                    TraceableFileIO.openOutputStreams(
                            p -> p.toString().equals(resourcePath.toString()));
            assertThat(read(table))
                    .extracting(row -> row.getString(1).toString())
                    .containsExactly("committed");
            assertSoftly(
                    softly -> {
                        softly.assertThat(closed.get()).isTrue();
                        softly.assertThat(openResources).isEmpty();
                        if (failCompaction) {
                            softly.assertThat(failure)
                                    .hasRootCauseMessage("expected compaction failure");
                            if (failRewriterClose) {
                                softly.assertThat(failure.getSuppressed())
                                        .anySatisfy(
                                                suppressed ->
                                                        assertThat(suppressed)
                                                                .hasMessage(
                                                                        "expected rewriter close failure"));
                            }
                        } else if (failRewriterClose) {
                            softly.assertThat(failure)
                                    .hasMessage("expected rewriter close failure");
                        } else {
                            softly.assertThat(failure).isNull();
                        }
                    });
            assertThat(read(table))
                    .extracting(row -> row.getString(1).toString())
                    .containsExactly("committed");
        } finally {
            // Keep the test process clean even when the close-under-test misses its owner.
            try {
                if (created.get() != null) {
                    if (!closed.get()) {
                        created.get().close();
                    }
                } else {
                    resource.close();
                }
            } catch (IOException ignored) {
            }
            commit.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }
}
