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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.paimon.shade.guava30.com.google.common.base.FinalizablePhantomReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the Driver-owned replay store, independently of streaming SQL integration. */
class DataFileMetaStoreTest {

    @TempDir Path directory;

    @Test
    void testBulkFilteringReplaysIndependentFilesAndPreservesSplitFieldsAndDeletionAlignment()
            throws Exception {
        try (DataFileMetaStore input = new DataFileMetaStore(directory, 1024 * 1024);
                DataFileMetaStore output = new DataFileMetaStore(directory, 1024 * 1024)) {
            for (int i = 0; i < 5; i++) {
                input.append(file(i));
            }
            input.seal();
            DeletionFile first = new DeletionFile("first.dv", 2, 12, 1L);
            DeletionFile third = new DeletionFile("third.dv", 20, 30, null);
            DataSplit withDeletes =
                    DataSplit.builder()
                            .withSnapshot(42)
                            .withPartition(BinaryRow.EMPTY_ROW)
                            .withBucket(3)
                            .withBucketPath("/unused/p/bucket-3")
                            .withTotalBuckets(7)
                            .withDataFiles(input.slice(0, 3))
                            .withDataDeletionFiles(Arrays.asList(first, null, third))
                            .isStreaming(true)
                            .rawConvertible(true)
                            .build();
            DataSplit withoutDeletes = split(input.slice(3, 5));
            java.util.function.Predicate<DataFileMeta> keep =
                    value ->
                            !value.fileName().equals("file-1")
                                    && !value.fileName().equals("file-3");
            List<DataSplit> expected =
                    Arrays.asList(
                            withDeletes.filterDataFile(keep).get(),
                            withoutDeletes.filterDataFile(keep).get());
            List<DataSplit> actual =
                    DataSplit.filterDataFiles(
                            Arrays.asList(withDeletes, withoutDeletes), keep, output);
            input.close();
            assertThat(actual).containsExactlyElementsOf(expected);
            assertThat(actual.get(0).deletionFiles().get()).containsExactly(first, third);
            assertThat(actual.get(1).deletionFiles()).isEmpty();
            assertThat(actual.get(0).isStreaming()).isTrue();
            assertThat(actual.get(0).totalBuckets()).isEqualTo(7);
            assertThat(actual.get(0).dataFiles().getClass().getName())
                    .contains("DataFileMetaStore$Slice");
            assertThat(actual.get(1).dataFiles()).containsExactly(file(4));
            output.close();
            assertThatThrownBy(() -> actual.get(0).dataFiles().get(0))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testBulkFilteringEmptyResultSealsAnEmptyReplayStore() throws Exception {
        try (DataFileMetaStore output = new DataFileMetaStore(directory, 8)) {
            assertThat(
                            DataSplit.filterDataFiles(
                                    Collections.singletonList(
                                            split(Arrays.asList(file(0), file(1)))),
                                    value -> false,
                                    output))
                    .isEmpty();
            assertThat(output.slice(0, 0)).isEmpty();
            assertThat(output.serializedBytes()).isEqualTo(8);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testBulkFilteringPredicateFailureDiscardsPartialOutputAndKeepsInputUsable()
            throws Exception {
        DataSplit source = split(Arrays.asList(file(0), file(1), file(2)));
        RuntimeException stopped = new RuntimeException("stop filtering");
        try (DataFileMetaStore output = new DataFileMetaStore(directory, 1024 * 1024)) {
            assertThatThrownBy(
                            () ->
                                    DataSplit.filterDataFiles(
                                            Collections.singletonList(source),
                                            value -> {
                                                if (value.fileName().equals("file-1")) {
                                                    throw stopped;
                                                }
                                                return true;
                                            },
                                            output))
                    .isSameAs(stopped);
            assertNoOwnedFiles();
            assertThat(source.dataFiles()).containsExactly(file(0), file(1), file(2));
        }
    }

    @Test
    void testBulkFilteringDiskFailureDoesNotExposeAPartialSplit() throws Exception {
        DataSplit source = split(Arrays.asList(file(0), file(1)));
        try (DataFileMetaStore output = new DataFileMetaStore(directory, 8)) {
            assertThatThrownBy(
                            () ->
                                    DataSplit.filterDataFiles(
                                            Collections.singletonList(source),
                                            value -> true,
                                            output))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("limit");
            assertNoOwnedFiles();
            assertThat(source.dataFiles()).containsExactly(file(0), file(1));
        }
    }

    @Test
    void testBulkFilteringRejectsUsedOutputWithoutClosingItsExistingContents() throws Exception {
        for (boolean sealed : new boolean[] {false, true}) {
            try (DataFileMetaStore output = new DataFileMetaStore(directory, 1024 * 1024)) {
                output.append(file(9));
                if (sealed) {
                    output.seal();
                }
                assertThatThrownBy(
                                () ->
                                        DataSplit.filterDataFiles(
                                                Collections.singletonList(
                                                        split(Collections.singletonList(file(0)))),
                                                value -> true,
                                                output))
                        .isInstanceOf(IllegalStateException.class);
                output.seal();
                assertThat(output.slice(0, 1)).containsExactly(file(9));
            }
        }
        assertNoOwnedFiles();
    }

    @Test
    void testAutomaticCleanupRequiresASealedStoreAndPreservesExplicitOwnership() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            assertThatThrownBy(store::withAutomaticCleanup)
                    .isInstanceOf(IllegalStateException.class);
            store.append(file(0));
            store.seal();
            assertThat(store.withAutomaticCleanup()).isSameAs(store);
            List<DataFileMeta> slice = store.slice(0, 1);
            assertThat(store.withAutomaticCleanup()).isSameAs(store);
            assertThat(slice).containsExactly(file(0));
            RandomAccessFile data = input(store, "dataInput");
            RandomAccessFile index = input(store, "indexInput");
            store.close();
            assertThat(data.getFD().valid()).isFalse();
            assertThat(index.getFD().valid()).isFalse();
            assertThatThrownBy(() -> slice.get(0)).isInstanceOf(IllegalStateException.class);
            // A queued notification after explicit close must remain harmless.
            cleanupNotification(store).finalizeReferent();
        }
        assertNoOwnedFiles();
    }

    @Test
    void testAutomaticCleanupNotificationClosesHandlesAndDeletesFilesWithoutForcedGc()
            throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.seal();
            store.withAutomaticCleanup();
            RandomAccessFile data = input(store, "dataInput");
            RandomAccessFile index = input(store, "indexInput");
            // Deterministically exercise the queue callback, without asserting GC timing.
            cleanupNotification(store).finalizeReferent();
            cleanupNotification(store).finalizeReferent();
            assertThat(data.getFD().valid()).isFalse();
            assertThat(index.getFD().valid()).isFalse();
            assertNoOwnedFiles();
        }
    }

    private static RandomAccessFile input(DataFileMetaStore store, String name) throws Exception {
        Field field = DataFileMetaStore.class.getDeclaredField(name);
        field.setAccessible(true);
        return (RandomAccessFile) field.get(store);
    }

    private static FinalizablePhantomReference<?> cleanupNotification(DataFileMetaStore store)
            throws Exception {
        Field field = DataFileMetaStore.class.getDeclaredField("cleanupReference");
        field.setAccessible(true);
        return (FinalizablePhantomReference<?>) field.get(store);
    }

    @Test
    void testSealedSlicesReplayIndependentlyAndAreImmutable() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            for (int i = 0; i < 10; i++) {
                store.append(file(i));
            }
            assertThatThrownBy(() -> store.slice(0, 10)).isInstanceOf(IllegalStateException.class);
            store.seal();
            List<DataFileMeta> slice = store.slice(2, 7);
            assertThat(slice).containsExactly(file(2), file(3), file(4), file(5), file(6));
            assertThat(slice.subList(1, 3)).containsExactly(file(3), file(4));
            Iterator<DataFileMeta> first = slice.iterator();
            Iterator<DataFileMeta> second = slice.iterator();
            assertThat(first.next()).isEqualTo(file(2));
            assertThat(first.next()).isEqualTo(file(3));
            assertThat(second.next()).isEqualTo(file(2));
            assertThat(slice.get(4)).isEqualTo(file(6));
            assertThat(first.next()).isEqualTo(file(4));
            assertThatThrownBy(() -> slice.add(file(11)))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> slice.set(0, file(11)))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> store.append(file(11)))
                    .isInstanceOf(IllegalStateException.class);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testBoundsAndEmptyStore() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 8)) {
            store.seal();
            assertThat(store.slice(0, 0)).isEmpty();
            assertThat(store.serializedBytes()).isEqualTo(8);
            assertThatThrownBy(() -> store.slice(-1, 0))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> store.slice(0, 1))
                    .isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> store.slice(1, 0))
                    .isInstanceOf(IndexOutOfBoundsException.class);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testSplitRetainsOnlyOwnedSliceButCopiesOrdinaryLists() throws Exception {
        List<DataFileMeta> ordinary = new ArrayList<>(Arrays.asList(file(0), file(1)));
        DataSplit copied = split(ordinary);
        ordinary.clear();
        assertThat(copied.dataFiles()).containsExactly(file(0), file(1));
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.append(file(1));
            store.seal();
            List<DataFileMeta> owned = store.slice(0, 2);
            assertThat(split(owned).dataFiles()).isSameAs(owned);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testBothSplitTransportsOutliveDriverStore() throws Exception {
        byte[] javaBytes;
        byte[] binaryBytes;
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.append(file(1));
            store.seal();
            DataSplit original = split(store.slice(0, 2));
            javaBytes = InstantiationUtil.serializeObject(original);
            binaryBytes = SplitSerializer.serialize(original);
        }
        assertNoOwnedFiles();
        DataSplit fromJava =
                InstantiationUtil.deserializeObject(javaBytes, getClass().getClassLoader());
        DataSplit fromBinary = (DataSplit) SplitSerializer.deserialize(binaryBytes);
        assertThat(fromJava).isEqualTo(split(Arrays.asList(file(0), file(1))));
        assertThat(fromBinary).isEqualTo(fromJava);
        assertThat(fromJava.dataFiles()).isInstanceOf(ArrayList.class);
        assertThat(fromBinary.dataFiles()).isInstanceOf(ArrayList.class);
    }

    @Test
    void testFileCountDoesNotGrowWithRecordCount() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 32 * 1024 * 1024)) {
            for (int i = 0; i < 20000; i++) {
                store.append(file(i));
            }
            store.seal();
            try (Stream<Path> files = Files.list(directory)) {
                List<Path> paths = files.collect(Collectors.toList());
                assertThat(paths).hasSize(2);
                long bytes = 0;
                for (Path path : paths) {
                    bytes += Files.size(path);
                }
                assertThat(store.serializedBytes()).isEqualTo(bytes);
            }
            Iterator<DataFileMeta> iterator = store.slice(0, 20000).iterator();
            int count = 0;
            while (iterator.hasNext()) {
                assertThat(iterator.next()).isEqualTo(file(count++));
            }
            assertThat(count).isEqualTo(20000);
            assertThat(store.slice(19999, 20000).get(0)).isEqualTo(file(19999));
        }
        assertNoOwnedFiles();
    }

    @Test
    void testDiskLimitFailureClosesStoreAndLeavesNoPartialFiles() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 8)) {
            assertThatThrownBy(() -> store.append(file(0)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("limit");
            assertNoOwnedFiles();
            assertThatThrownBy(store::seal).isInstanceOf(IllegalStateException.class);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testDiskLimitIncludesIndexAndEndOffset() throws Exception {
        DataOutputSerializer serialized = new DataOutputSerializer(128);
        new DataFileMetaSerializer().serialize(file(0), serialized);
        try (DataFileMetaStore store = new DataFileMetaStore(directory, serialized.length() + 8L)) {
            assertThatThrownBy(() -> store.append(file(0)))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("limit");
            assertNoOwnedFiles();
        }
    }

    @Test
    void testReadFailureIsNotReportedAsAnEmptySlice() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.append(file(1));
            store.seal();
            Path data;
            try (Stream<Path> files = Files.list(directory)) {
                data = files.filter(path -> path.toString().endsWith(".data")).findFirst().get();
            }
            try (FileChannel channel = FileChannel.open(data, StandardOpenOption.WRITE)) {
                channel.truncate(0);
            }
            List<DataFileMeta> slice = store.slice(0, 2);
            assertThatThrownBy(() -> slice.get(0)).isInstanceOf(UncheckedIOException.class);
            assertThatThrownBy(() -> slice.iterator().next())
                    .isInstanceOf(UncheckedIOException.class);
        }
        assertNoOwnedFiles();
    }

    @Test
    void testInterruptedDataReplayDoesNotPoisonIndependentSlices() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.append(file(1));
            store.seal();
            List<DataFileMeta> interruptedSlice = store.slice(0, 2);
            List<DataFileMeta> independentSlice = store.slice(0, 2);
            // Resolve index offsets before interruption, but leave both data buffers empty.
            Iterator<DataFileMeta> interrupted = interruptedSlice.iterator();
            Iterator<DataFileMeta> independent = independentSlice.iterator();

            assertInterruptedReadFails(interrupted::next);

            assertThat(independent.next()).isEqualTo(file(0));
            assertThat(independent.next()).isEqualTo(file(1));
            assertThat(independent.hasNext()).isFalse();
            assertThat(independentSlice).containsExactly(file(0), file(1));
            assertThat(independentSlice.get(0)).isEqualTo(file(0));
            assertThat(independentSlice.get(1)).isEqualTo(file(1));

            Iterator<DataFileMeta> buffered = interruptedSlice.iterator();
            assertThat(buffered.next()).isEqualTo(file(0));
            assertInterruptedReadFails(buffered::next);
            assertThat(independentSlice).containsExactly(file(0), file(1));
        }
        assertNoOwnedFiles();
    }

    @Test
    void testInterruptedIndexReadDoesNotPoisonIndependentSlices() throws Exception {
        try (DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024)) {
            store.append(file(0));
            store.append(file(1));
            store.seal();
            List<DataFileMeta> interruptedSlice = store.slice(0, 2);
            List<DataFileMeta> independentSlice = store.slice(0, 2);

            assertInterruptedReadFails(() -> interruptedSlice.get(0));

            assertThat(independentSlice.get(0)).isEqualTo(file(0));
            assertThat(independentSlice.get(1)).isEqualTo(file(1));
            assertThat(independentSlice).containsExactly(file(0), file(1));
        }
        assertNoOwnedFiles();
    }

    private static void assertInterruptedReadFails(Runnable read) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(
                            () -> {
                                Thread.currentThread().interrupt();
                                assertThatThrownBy(read::run)
                                        .isInstanceOf(UncheckedIOException.class);
                                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                            })
                    .get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentReplayAndCloseInvalidation() throws Exception {
        DataFileMetaStore store = new DataFileMetaStore(directory, 1024 * 1024);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 100; i++) {
                store.append(file(i));
            }
            store.seal();
            List<DataFileMeta> slice = store.slice(10, 90);
            List<Callable<Integer>> reads = new ArrayList<>();
            for (int task = 0; task < 4; task++) {
                reads.add(
                        () -> {
                            int count = 0;
                            for (DataFileMeta value : slice) {
                                assertThat(value).isEqualTo(file(10 + count++));
                                assertThat(slice.get(79)).isEqualTo(file(89));
                            }
                            return count;
                        });
            }
            for (Future<Integer> result : executor.invokeAll(reads)) {
                assertThat(result.get()).isEqualTo(80);
            }
            Iterator<DataFileMeta> iterator = slice.iterator();
            assertThat(iterator.next()).isEqualTo(file(10));
            assertThat(iterator.hasNext()).isTrue();
            store.close();
            assertThatThrownBy(() -> slice.get(0)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(iterator::next).isInstanceOf(IllegalStateException.class);
        } finally {
            executor.shutdownNow();
            store.close();
        }
        assertNoOwnedFiles();
    }

    private void assertNoOwnedFiles() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            assertThat(files.count()).isZero();
        }
    }

    private static DataFileMeta file(int index) {
        return DataFileMeta.create(
                "file-" + index,
                100L + index,
                2,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                index,
                index + 1L,
                0,
                0,
                Collections.singletonList("extra-" + index),
                Timestamp.fromEpochMillis(12345),
                0L,
                new byte[] {1, 2, 3},
                FileSource.APPEND,
                Arrays.asList("id", "value"),
                "s3://not-opened/" + index,
                2L * index,
                Arrays.asList("id", "value"),
                new long[] {index, index + 1L});
    }

    private static DataSplit split(List<DataFileMeta> files) {
        return DataSplit.builder()
                .withSnapshot(17)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("/unused-on-executor/bucket-0")
                .withDataFiles(files)
                .rawConvertible(false)
                .build();
    }
}
