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
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.CollectedDeletes;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroWriter;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaTestBase;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.testutils.assertj.PaimonAssertions;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/** Tests cleanup when manifest rewrites fail with {@link Error}. */
class ManifestRewriteCleanupTest extends ManifestFileMetaTestBase {

    private static final RowType PARTITION_TYPE = RowType.of(DataTypes.INT());

    @TempDir java.nio.file.Path tempDir;

    private FailingFileIO fileIO;
    private ManifestFile manifestFile;
    private Path manifestPath;

    @BeforeEach
    void beforeEach() {
        fileIO = new FailingFileIO();
        manifestFile = createManifestFile(tempDir.toString(), fileIO);
        manifestPath = new Path(tempDir.toString(), "manifest");
    }

    @Test
    void testRunMergeAbortsWriterAndPreservesError() throws Exception {
        ManifestFileMeta input =
                makeManifest(
                        rowIdEntry(FileKind.ADD, "file-0", 0),
                        rowIdEntry(FileKind.ADD, "file-1", 1));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("cursor failure");
        IOException closeFailure = new IOException("cursor close failure");
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();
        CollectedDeletes deletes = new CollectedDeletes(true);

        Throwable thrown;
        try {
            ManifestEntryRunMergePlan plan =
                    runMergePlan(input, deletes, false, 1, primaryFailure, closeFailure);
            thrown = catchThrowable(() -> plan.mergeToManifest(manifestFile, newFilesForAbort));
        } finally {
            deletes.release();
        }

        assertThat(thrown).isSameAs(primaryFailure).hasSuppressedException(closeFailure);
        assertThat(newFilesForAbort).isEmpty();
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testRunMergeAbortsBothWritersWhenCleanupFails() throws Exception {
        ManifestFileMeta input =
                makeManifest(
                        rowIdEntry(FileKind.ADD, "add", 0),
                        rowIdEntry(FileKind.DELETE, "delete-0", 1),
                        rowIdEntry(FileKind.DELETE, "delete-1", 2));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("cursor failure");
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();
        CollectedDeletes deletes = new CollectedDeletes(true);
        fileIO.failDeletes();

        Throwable thrown;
        try {
            ManifestEntryRunMergePlan plan =
                    runMergePlan(input, deletes, true, 2, primaryFailure, null);
            thrown =
                    catchThrowable(() -> plan.mergeMinorToManifest(manifestFile, newFilesForAbort));
        } finally {
            deletes.release();
        }

        assertThat(thrown).isSameAs(primaryFailure);
        assertThat(thrown.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("delete failure 1", "delete failure 2");
        assertThat(fileIO.deleteAttempts()).isEqualTo(2);
        assertThat(newFilesForAbort).isEmpty();
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testExternalSortAbortsWriterAndPreservesError() throws Exception {
        List<ManifestFileMeta> input =
                Collections.singletonList(
                        makeManifest(
                                rowIdEntry(FileKind.ADD, "file-0", 0),
                                rowIdEntry(FileKind.ADD, "file-1", 1)));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("sort key failure");
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();
        CollectedDeletes deletes = new CollectedDeletes(true);

        Throwable thrown;
        try {
            thrown =
                    catchThrowable(
                            () ->
                                    ManifestEntryExternalSort.sortAndWriteFullEntries(
                                            input,
                                            failingSortKey(input, 1, primaryFailure),
                                            externalSortConfig(),
                                            manifestFile,
                                            newFilesForAbort,
                                            deletes,
                                            1));
        } finally {
            deletes.release();
        }

        assertThat(thrown).isSameAs(primaryFailure);
        assertThat(newFilesForAbort).isEmpty();
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testExternalSortAbortsBothWritersWhenCleanupFails() throws Exception {
        List<ManifestFileMeta> input =
                Collections.singletonList(
                        makeManifest(
                                rowIdEntry(FileKind.ADD, "add", 0),
                                rowIdEntry(FileKind.DELETE, "delete-0", 1),
                                rowIdEntry(FileKind.DELETE, "delete-1", 2)));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("sort key failure");
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();
        fileIO.failDeletes();

        Throwable thrown =
                catchThrowable(
                        () ->
                                ManifestEntryExternalSort.sortAndWriteMinorEntries(
                                        input,
                                        failingSortKey(input, 2, primaryFailure),
                                        externalSortConfig(),
                                        manifestFile,
                                        newFilesForAbort,
                                        1));

        assertThat(thrown).isSameAs(primaryFailure);
        assertThat(thrown.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("delete failure 1", "delete failure 2");
        assertThat(fileIO.deleteAttempts()).isEqualTo(2);
        assertThat(newFilesForAbort).isEmpty();
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testLegacyMergeAbortsActiveWriterOnError() throws Exception {
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(makeEntry(true, "file-0")),
                        makeManifest(makeEntry(true, "file-1")));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("legacy writer failure");
        ManifestFile spyManifestFile = spy(manifestFile);
        ManifestAvroWriter activeWriter = spy(manifestFile.createAvroWriter());
        doReturn(activeWriter).when(spyManifestFile).createAvroWriter();
        doAnswer(
                        invocation -> {
                            Iterable<? extends ManifestEntry> entries = invocation.getArgument(0);
                            activeWriter.write(entries.iterator().next());
                            throw primaryFailure;
                        })
                .when(activeWriter)
                .write(ArgumentMatchers.<Iterable<? extends ManifestEntry>>any());

        Throwable thrown =
                catchThrowable(
                        () ->
                                ManifestFileLegacyMerger.tryFullCompaction(
                                        input,
                                        new ArrayList<>(),
                                        spyManifestFile,
                                        Long.MAX_VALUE,
                                        1,
                                        PARTITION_TYPE,
                                        1));

        assertThat(thrown).isSameAs(primaryFailure);
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testBlockMergePreservesErrorWhenWriterCleanupFails() throws Exception {
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(makeEntry(true, "file-0")),
                        makeManifest(makeEntry(true, "file-1")));
        int manifestCount = manifestFileCount();
        AssertionError primaryFailure = new AssertionError("block writer failure");
        ManifestFile spyManifestFile = spy(manifestFile);
        ManifestAvroWriter activeWriter = spy(manifestFile.createAvroWriter());
        doReturn(activeWriter).when(spyManifestFile).createAvroWriter();
        doAnswer(
                        invocation -> {
                            activeWriter.write(makeEntry(true, "partial"));
                            throw primaryFailure;
                        })
                .when(activeWriter)
                .writeEncodedManifest(
                        ArgumentMatchers.any(ManifestAvroReader.class),
                        ArgumentMatchers.any(ManifestFileMeta.class));
        fileIO.failDeletes();

        Throwable thrown =
                catchThrowable(
                        () ->
                                ManifestFileBlockMerger.tryFullCompaction(
                                        input,
                                        new ArrayList<>(),
                                        spyManifestFile,
                                        Long.MAX_VALUE,
                                        1,
                                        PARTITION_TYPE,
                                        1));

        assertThat(thrown)
                .satisfies(
                        PaimonAssertions.anyCauseMatches(
                                primaryFailure.getClass(), primaryFailure.getMessage()));
        assertThat(thrown.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("delete failure 1");
        assertThat(fileIO.deleteAttempts()).isEqualTo(1);
        assertNoManifestLeak(manifestCount);
    }

    @Test
    void testParallelBlockRewriteQuiescesBeforeReleasingDeletes() throws Exception {
        ManifestFileMeta first = makeManifest(makeEntry(true, "first"));
        ManifestFileMeta slow = makeManifest(makeEntry(true, "slow"));
        List<ManifestFileMeta> input = Arrays.asList(first, slow);
        int manifestCount = manifestFileCount();

        AssertionError writerFailure = new AssertionError("block writer failure");
        RuntimeException planningFailure = new RuntimeException("planning failure");
        CountDownLatch slowWorkerStarted = new CountDownLatch(1);
        CountDownLatch slowWorkerInterrupted = new CountDownLatch(1);
        CountDownLatch allowSlowWorkerToExit = new CountDownLatch(1);
        AtomicBoolean slowWorkerExited = new AtomicBoolean();
        AtomicBoolean deletesReleased = new AtomicBoolean();
        AtomicBoolean releaseObservedWorkerExit = new AtomicBoolean();

        CollectedDeletes deletes = mock(CollectedDeletes.class);
        when(deletes.isEmpty()).thenReturn(false);
        when(deletes.useRowIdFilter()).thenReturn(false);
        doAnswer(
                        invocation -> {
                            ProjectedManifestEntry entry = invocation.getArgument(0);
                            if ("first".equals(entry.file().fileName())) {
                                await(slowWorkerStarted);
                                return true;
                            }
                            slowWorkerStarted.countDown();
                            try {
                                awaitIgnoringInterrupts(
                                        allowSlowWorkerToExit, slowWorkerInterrupted);
                            } finally {
                                slowWorkerExited.set(true);
                            }
                            throw planningFailure;
                        })
                .when(deletes)
                .copyable(
                        ArgumentMatchers.any(ProjectedManifestEntry.class),
                        ArgumentMatchers.any(ReusableIdentifier.class),
                        ArgumentMatchers.eq(false));
        doAnswer(
                        invocation -> {
                            deletesReleased.set(true);
                            releaseObservedWorkerExit.set(slowWorkerExited.get());
                            return null;
                        })
                .when(deletes)
                .release();

        ManifestFile spyManifestFile = spy(manifestFile);
        ManifestAvroWriter activeWriter = spy(manifestFile.createAvroWriter());
        doReturn(activeWriter).when(spyManifestFile).createAvroWriter();
        doAnswer(
                        invocation -> {
                            activeWriter.write(makeEntry(true, "partial"));
                            throw writerFailure;
                        })
                .when(activeWriter)
                .writeEncodedBlock(
                        ArgumentMatchers.any(AvroRawBlock.class),
                        ArgumentMatchers.any(EncodedBlockMeta.class));
        fileIO.failDeletes();

        ExecutorService caller = Executors.newSingleThreadExecutor();
        Future<Throwable> rewriteResult =
                caller.submit(
                        () -> {
                            try {
                                return catchThrowable(
                                        () ->
                                                invokeBlockRewrite(
                                                        input, spyManifestFile, deletes, 2));
                            } finally {
                                deletes.release();
                            }
                        });

        try {
            assertThat(slowWorkerStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(slowWorkerInterrupted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(slowWorkerExited).isFalse();
            assertThat(deletesReleased).isFalse();
            assertThat(rewriteResult.isDone()).isFalse();

            allowSlowWorkerToExit.countDown();
            Throwable thrown = rewriteResult.get(3, TimeUnit.SECONDS);

            assertThat(thrown).isSameAs(writerFailure);
            assertThat(slowWorkerExited).isTrue();
            assertThat(deletesReleased).isTrue();
            assertThat(releaseObservedWorkerExit).isTrue();
            assertThat(thrown.getSuppressed())
                    .extracting(Throwable::getMessage)
                    .containsExactly(
                            "Failed to plan manifest rewrite for " + slow.fileName(),
                            "delete failure 1");
            assertThat(thrown.getSuppressed()[0].getCause()).isSameAs(planningFailure);
            assertThat(fileIO.deleteAttempts()).isEqualTo(1);
            assertNoManifestLeak(manifestCount);
        } finally {
            allowSlowWorkerToExit.countDown();
            caller.shutdownNow();
            assertThat(caller.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testWriterPreservesWriteErrorAndCleansAllRollingFiles() throws Exception {
        ManifestAvroWriter writer = createManifestFile(1).createAvroWriter();
        ManifestEntry entry = rowIdEntry(FileKind.ADD, "file", 0);
        // ManifestAvroWriter checks the rolling threshold every 1,000 records.
        writer.write(Collections.nCopies(2000, entry));
        assertThat(manifestFileCount()).isEqualTo(2);

        AssertionError primaryFailure = new AssertionError("manifest write failure");
        AtomicInteger kindCalls = new AtomicInteger();
        ManifestEntry failingEntry =
                mock(ManifestEntry.class, AdditionalAnswers.delegatesTo(entry));
        doAnswer(
                        invocation -> {
                            if (kindCalls.incrementAndGet() == 2) {
                                throw primaryFailure;
                            }
                            return entry.kind();
                        })
                .when(failingEntry)
                .kind();
        fileIO.failNextDeleteBeforeDeletion();

        Throwable thrown = catchThrowable(() -> writer.write(failingEntry));

        assertThat(thrown).isSameAs(primaryFailure);
        assertThat(thrown.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("delete failure 1");
        assertThat(fileIO.deleteAttempts()).isEqualTo(3);
        assertThat(manifestFileCount()).isEqualTo(1);
        assertThat(
                        TraceableFileIO.openOutputStreams(
                                path -> path.toString().startsWith(manifestPath.toString())))
                .isEmpty();
    }

    @Test
    void testCloseCursorsContinuesAfterError() {
        AssertionError primaryFailure = new AssertionError("first close failure");
        IOException cleanupFailure = new IOException("second close failure");
        CloseFailureCursor first = new CloseFailureCursor(primaryFailure);
        CloseFailureCursor second = new CloseFailureCursor(cleanupFailure);
        CloseFailureCursor third = new CloseFailureCursor(null);

        Throwable thrown =
                catchThrowable(
                        () ->
                                ManifestEntryRunMergePlan.closeCursors(
                                        Arrays.asList(first, second, third)));

        assertThat(thrown).isSameAs(primaryFailure).hasSuppressedException(cleanupFailure);
        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
        assertThat(third.closed).isTrue();
    }

    @Test
    void testManifestFileMergerPreservesErrorWhenCleanupFails() throws IOException {
        List<ManifestFileMeta> input =
                Arrays.asList(
                        makeManifest(makeEntry(true, "file-0")),
                        makeManifest(makeEntry(true, "file-1")),
                        makeManifest(makeEntry(true, "file-2")),
                        makeManifest(makeEntry(true, "file-3")));
        int manifestCount = manifestFileCount();
        long targetSize =
                input.stream().mapToLong(ManifestFileMeta::fileSize).max().getAsLong() + 1;
        AssertionError primaryFailure = new AssertionError("manifest merge failure");
        fileIO.failDeletes();

        Options options = new Options();
        options.set(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), targetSize + "B");
        options.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), Long.MAX_VALUE + "B");
        options.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 2);
        options.set(CoreOptions.MANIFEST_MERGE_OPTIMIZE_ENABLED, false);
        options.set(CoreOptions.SCAN_MANIFEST_PARALLELISM, 1);

        // The first iteration checks full compaction. Fail in the second iteration after
        // compactMinor registers one file.
        List<ManifestFileMeta> failingInput = new FailingSecondIterationList(input, primaryFailure);
        Throwable thrown =
                catchThrowable(
                        () ->
                                ManifestFileMerger.merge(
                                        failingInput,
                                        manifestFile,
                                        PARTITION_TYPE,
                                        new CoreOptions(options)));

        assertThat(thrown).isSameAs(primaryFailure);
        assertThat(thrown.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("delete failure 1");
        assertThat(fileIO.deleteAttempts()).isEqualTo(1);
        assertNoManifestLeak(manifestCount);
    }

    private void invokeBlockRewrite(
            List<ManifestFileMeta> input,
            ManifestFile rewriteManifestFile,
            CollectedDeletes deletes,
            int parallelism)
            throws Exception {
        Method rewriteManifests =
                ManifestFileBlockMerger.class.getDeclaredMethod(
                        "rewriteManifests",
                        List.class,
                        ManifestFile.class,
                        RowType.class,
                        CollectedDeletes.class,
                        boolean.class,
                        Filter.class,
                        List.class,
                        Integer.class);
        rewriteManifests.setAccessible(true);
        try {
            rewriteManifests.invoke(
                    null,
                    input,
                    rewriteManifestFile,
                    PARTITION_TYPE,
                    deletes,
                    false,
                    null,
                    null,
                    parallelism);
        } catch (InvocationTargetException e) {
            Throwable failure = e.getCause();
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(3, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for the parallel planning worker.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch, CountDownLatch interrupted) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        }
    }

    private ManifestEntryRunMergePlan runMergePlan(
            ManifestFileMeta input,
            CollectedDeletes deletes,
            boolean minor,
            int successfulAdvancesBeforeFailure,
            AssertionError failure,
            IOException closeFailure) {
        ManifestEntryRunMerge.SortPartitionDictionary partitions =
                new ManifestEntryRunMerge.SortPartitionDictionary(
                        (left, right) -> Integer.compare(left.getInt(0), right.getInt(0)));
        ManifestEntryRunMergePlan.Source.Spec source =
                (file, planDeletes, planMinor, planPartitions) ->
                        new FailingCursor(
                                new ManifestEntryRunMergePlan.InMemoryManifestCursor(
                                        file, input, planDeletes, planMinor, planPartitions),
                                successfulAdvancesBeforeFailure,
                                failure,
                                closeFailure);
        return new ManifestEntryRunMergePlan(
                Collections.singletonList(source), partitions, deletes, minor);
    }

    private ManifestFileSorter.ManifestSortKey failingSortKey(
            List<ManifestFileMeta> input, int successfulRowsBeforeFailure, AssertionError failure) {
        return new FailingManifestSortKey(
                ManifestFileSorter.createSortKey(true, input, null, PARTITION_TYPE),
                successfulRowsBeforeFailure,
                failure);
    }

    private ManifestEntryExternalSort.ExternalSortConfig externalSortConfig() {
        Options options = new Options();
        options.set(CoreOptions.SORT_SPILL_BUFFER_SIZE.key(), "1 mb");
        return ManifestEntryExternalSort.ExternalSortConfig.from(new CoreOptions(options), null);
    }

    private ManifestEntry rowIdEntry(FileKind kind, String fileName, long firstRowId) {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, 0);
        writer.complete();
        return ManifestEntry.create(
                kind,
                partition,
                0,
                0,
                DataFileMeta.create(
                        fileName,
                        0,
                        1,
                        partition,
                        partition,
                        StatsTestUtils.newEmptySimpleStats(),
                        StatsTestUtils.newEmptySimpleStats(),
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        Timestamp.fromEpochMillis(200000),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        firstRowId,
                        Collections.singletonList("f0"),
                        null));
    }

    private ManifestFile createManifestFile(long suggestedFileSize) {
        Path tablePath = new Path(tempDir.toString());
        return new ManifestFile.Factory(
                        fileIO,
                        new SchemaManager(fileIO, tablePath),
                        PARTITION_TYPE,
                        avro,
                        "zstd",
                        new FileStorePathFactory(
                                tablePath,
                                PARTITION_TYPE,
                                "default",
                                CoreOptions.FILE_FORMAT.defaultValue(),
                                CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                                CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                                CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                                CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                                CoreOptions.FILE_COMPRESSION.defaultValue(),
                                null,
                                null,
                                CoreOptions.ExternalPathStrategy.NONE,
                                null,
                                false,
                                null),
                        suggestedFileSize,
                        null)
                .create();
    }

    private int manifestFileCount() throws IOException {
        return fileIO.listStatus(manifestPath).length;
    }

    private void assertNoManifestLeak(int expectedManifestCount) throws IOException {
        assertThat(manifestFileCount()).isEqualTo(expectedManifestCount);
        assertThat(
                        TraceableFileIO.openOutputStreams(
                                path -> path.toString().startsWith(manifestPath.toString())))
                .isEmpty();
    }

    @Override
    protected ManifestFile getManifestFile() {
        return manifestFile;
    }

    @Override
    protected RowType getPartitionType() {
        return PARTITION_TYPE;
    }

    private static class FailingCursor implements ManifestEntryRunMergePlan.Cursor {

        private final ManifestEntryRunMergePlan.Cursor delegate;
        private final int successfulAdvancesBeforeFailure;
        private final AssertionError failure;
        private final IOException closeFailure;
        private int successfulAdvances;

        private FailingCursor(
                ManifestEntryRunMergePlan.Cursor delegate,
                int successfulAdvancesBeforeFailure,
                AssertionError failure,
                IOException closeFailure) {
            this.delegate = delegate;
            this.successfulAdvancesBeforeFailure = successfulAdvancesBeforeFailure;
            this.failure = failure;
            this.closeFailure = closeFailure;
        }

        @Override
        public boolean advance() throws Exception {
            if (successfulAdvances >= successfulAdvancesBeforeFailure) {
                throw failure;
            }
            boolean advanced = delegate.advance();
            if (advanced) {
                successfulAdvances++;
            }
            return advanced;
        }

        @Override
        public boolean hasCurrent() {
            return delegate.hasCurrent();
        }

        @Override
        public ProjectedManifestEntry current() {
            return delegate.current();
        }

        @Override
        public EncodedEntry metadata() {
            return delegate.metadata();
        }

        @Override
        public ManifestEntryRunMerge.SortKey key() {
            return delegate.key();
        }

        @Override
        public ByteBuffer encodedRecord() {
            return delegate.encodedRecord();
        }

        @Override
        public ReusableIdentifier identifier() {
            return delegate.identifier();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static class FailingManifestSortKey implements ManifestFileSorter.ManifestSortKey {

        private final ManifestFileSorter.ManifestSortKey delegate;
        private final int successfulRowsBeforeFailure;
        private final AssertionError failure;
        private int successfulRows;

        private FailingManifestSortKey(
                ManifestFileSorter.ManifestSortKey delegate,
                int successfulRowsBeforeFailure,
                AssertionError failure) {
            this.delegate = delegate;
            this.successfulRowsBeforeFailure = successfulRowsBeforeFailure;
            this.failure = failure;
        }

        @Override
        public int compareMin(ManifestFileMeta left, ManifestFileMeta right) {
            return delegate.compareMin(left, right);
        }

        @Override
        public int compareMax(ManifestFileMeta left, ManifestFileMeta right) {
            return delegate.compareMax(left, right);
        }

        @Override
        public boolean isAfterMax(ManifestFileMeta file, ManifestFileMeta maxFile) {
            return delegate.isAfterMax(file, maxFile);
        }

        @Override
        public RowType externalSortRowType() {
            return delegate.externalSortRowType();
        }

        @Override
        public int[] externalSortKeyFields() {
            return delegate.externalSortKeyFields();
        }

        @Override
        public void replaceExternalSortRow(
                GenericRow row, ManifestEntry entry, InternalRow binaryManifestRow) {
            delegate.replaceExternalSortRow(row, entry, binaryManifestRow);
        }

        @Override
        public InternalRow binaryManifestRow(BinaryRow row) {
            if (successfulRows >= successfulRowsBeforeFailure) {
                throw failure;
            }
            successfulRows++;
            return delegate.binaryManifestRow(row);
        }
    }

    private static class CloseFailureCursor implements ManifestEntryRunMergePlan.Cursor {

        private final Throwable failure;
        private boolean closed;

        private CloseFailureCursor(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public boolean advance() {
            return false;
        }

        @Override
        public boolean hasCurrent() {
            return false;
        }

        @Override
        public ProjectedManifestEntry current() {
            return null;
        }

        @Override
        public EncodedEntry metadata() {
            return null;
        }

        @Override
        public ManifestEntryRunMerge.SortKey key() {
            return null;
        }

        @Override
        public ByteBuffer encodedRecord() {
            return null;
        }

        @Override
        public ReusableIdentifier identifier() {
            return null;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            if (failure != null) {
                throw (Exception) failure;
            }
        }
    }

    private static class FailingFileIO extends TraceableFileIO {

        private boolean trackDeletes;
        private boolean failDeletesAfterDeletion;
        private int failuresBeforeDeletion;
        private int deleteAttempts;

        private void failDeletes() {
            trackDeletes = true;
            failDeletesAfterDeletion = true;
        }

        private void failNextDeleteBeforeDeletion() {
            trackDeletes = true;
            failuresBeforeDeletion = 1;
        }

        private int deleteAttempts() {
            return deleteAttempts;
        }

        @Override
        public boolean delete(Path file, boolean recursive) throws IOException {
            if (trackDeletes) {
                deleteAttempts++;
                if (failuresBeforeDeletion > 0) {
                    failuresBeforeDeletion--;
                    throw new RuntimeException("delete failure " + deleteAttempts);
                }
            }
            boolean deleted = super.delete(file, recursive);
            if (failDeletesAfterDeletion) {
                throw new RuntimeException("delete failure " + deleteAttempts);
            }
            return deleted;
        }
    }

    private static class FailingSecondIterationList extends AbstractList<ManifestFileMeta> {

        private final List<ManifestFileMeta> delegate;
        private final AssertionError failure;
        private int iterationCount;

        private FailingSecondIterationList(
                List<ManifestFileMeta> delegate, AssertionError failure) {
            this.delegate = delegate;
            this.failure = failure;
        }

        @Override
        public ManifestFileMeta get(int index) {
            return delegate.get(index);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Iterator<ManifestFileMeta> iterator() {
            Iterator<ManifestFileMeta> iterator = delegate.iterator();
            int currentIteration = ++iterationCount;
            return new Iterator<ManifestFileMeta>() {

                private int returned;

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public ManifestFileMeta next() {
                    if (currentIteration == 2 && returned == 2) {
                        throw failure;
                    }
                    returned++;
                    return iterator.next();
                }
            };
        }
    }
}
