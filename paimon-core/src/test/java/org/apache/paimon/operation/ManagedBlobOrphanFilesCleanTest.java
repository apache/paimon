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
import org.apache.paimon.Snapshot;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector;
import org.apache.paimon.blob.ManagedBlobReachabilityCollector.Result;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.blob.ManagedBlobReferenceFile.Reference;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests orphan-file cleanup of unreferenced primary-key managed BLOB packs. */
public class ManagedBlobOrphanFilesCleanTest extends TableTestBase {

    @Test
    public void testDeleteUnreferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("orphan_pack");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));

        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);
        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
        assertThat(managedBlobs(table)).isNotEmpty();
    }

    @Test
    public void testDeleteFailureIsNotReported() throws Exception {
        FileStoreTable table = createManagedBlobTable("delete_failure");
        Path orphan = new Path(bucketPath(table), "delete-failure.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        FileIO deleteFailingFileIO =
                new LocalFileIO() {
                    @Override
                    public boolean delete(Path path, boolean recursive) throws IOException {
                        if (path.getName().equals(orphan.getName())) {
                            return false;
                        }
                        return super.delete(path, recursive);
                    }
                };
        FileStoreTable failingTable =
                FileStoreTableFactory.create(deleteFailingFileIO, table.location(), table.schema());

        CleanOrphanFilesResult result =
                new LocalManagedBlobOrphanFilesClean(
                                failingTable,
                                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2),
                                false)
                        .clean();

        assertThat(result.getDeletedFileCount()).isZero();
        assertThat(result.getDeletedFileTotalLenInBytes()).isZero();
        assertThat(result.getDeletedFilesPath()).isEmpty();
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testKeepReferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("keep_pack");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {9, 8, 7})));
        List<Path> before = managedBlobs(table);
        assertThat(before).isNotEmpty();

        List<Path> deleted = clean(table);

        assertThat(deleted).doesNotContainAnyElementsOf(before);
        for (Path pack : before) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData())
                .containsExactly((byte) 9, (byte) 8, (byte) 7);
    }

    @Test
    public void testPackIdentityIgnoresFileSystemQualification() {
        Path listed = new Path("file:/tmp/table/bucket-0/data-a.managed.blob");
        Path referenced = new Path("traceable:/tmp/table/bucket-0/data-a.managed.blob");
        Path unqualifiedHdfs = new Path("hdfs:///tmp/table/bucket-0/data-a.managed.blob");
        Path qualifiedHdfs =
                new Path("hdfs://namenode:8020/tmp/table/bucket-0/data-a.managed.blob");
        Path otherBucket = new Path("file:/tmp/table/bucket-1/data-a.managed.blob");
        assertThat(ManagedBlobOrphanFilesClean.packIdentity(referenced))
                .isEqualTo(ManagedBlobOrphanFilesClean.packIdentity(listed));
        assertThat(ManagedBlobOrphanFilesClean.packIdentity(unqualifiedHdfs))
                .isEqualTo(ManagedBlobOrphanFilesClean.packIdentity(qualifiedHdfs));
        assertThat(ManagedBlobOrphanFilesClean.packIdentity(otherBucket))
                .isNotEqualTo(ManagedBlobOrphanFilesClean.packIdentity(listed));
    }

    @Test
    public void testRelativeTablePathDoesNotDeleteReferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("relative_table_path");
        java.nio.file.Path absoluteLocation =
                Paths.get(table.location().toUri().getPath()).toAbsolutePath();
        String relativeLocation =
                Paths.get("").toAbsolutePath().relativize(absoluteLocation).toString();
        FileStoreTable relativeTable =
                FileStoreTableFactory.create(
                        new LocalFileIO(), new Path(relativeLocation), table.schema());
        write(
                relativeTable,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        List<Path> referenced = managedBlobs(relativeTable);
        assertThat(referenced).isNotEmpty();

        CleanOrphanFilesResult result;
        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(
                        relativeTable,
                        System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2),
                        false)) {
            result = cleaner.clean();
        }

        assertThat(result.getDeletedFilesPath()).isEmpty();
        for (Path pack : referenced) {
            assertThat(relativeTable.fileIO().exists(pack)).isTrue();
        }
        assertThat(read(relativeTable)).hasSize(1);
    }

    @Test
    public void testUnresolvedNonLocalRelativeListingSkipsPackGc() throws Exception {
        FileStoreTable table = createManagedBlobTable("non_local_relative_listing");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        List<Path> packs = managedBlobs(table);

        RelativeListingFileIO relativeListingFileIO = new RelativeListingFileIO();
        FileStoreTable relativeListingTable =
                FileStoreTableFactory.create(
                        relativeListingFileIO, table.location(), table.schema());
        CleanOrphanFilesResult result;
        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(relativeListingTable, Long.MAX_VALUE, false)) {
            result = cleaner.clean();
        }

        assertThat(result.getDeletedFilesPath()).isEmpty();
        assertThat(relativeListingFileIO.managedBlobDeleteAttempts()).isZero();
        for (Path pack : packs) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @Test
    public void testRepeatedCleanDoesNotAccumulateResults() throws Exception {
        FileStoreTable table = createManagedBlobTable("repeated_clean");
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().mkdirs(orphan.getParent());
        table.fileIO().newOutputStream(orphan, false).close();

        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, true)) {
            CleanOrphanFilesResult first = cleaner.clean();
            CleanOrphanFilesResult second = cleaner.clean();

            assertThat(first.getDeletedFilesPath())
                    .extracting(Path::getName)
                    .containsExactly(orphan.getName());
            assertThat(second.getDeletedFilesPath())
                    .extracting(Path::getName)
                    .containsExactly(orphan.getName());
            assertThat(first.getDeletedFileCount()).isEqualTo(1);
            assertThat(second.getDeletedFileCount()).isEqualTo(1);
        }
    }

    @Test
    public void testQualifiedListingDoesNotDeleteReferencedPack() throws Exception {
        FileStoreTable table = createManagedBlobTable("qualified_listing");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        List<Path> referenced = managedBlobs(table);
        assertThat(referenced).isNotEmpty();

        QualifiedListingFileIO qualifiedFileIO = new QualifiedListingFileIO();
        FileStoreTable qualifiedTable =
                FileStoreTableFactory.create(qualifiedFileIO, table.location(), table.schema());
        CleanOrphanFilesResult result;
        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(
                        qualifiedTable,
                        System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2),
                        false)) {
            result = cleaner.clean();
        }

        assertThat(result.getDeletedFilesPath()).isEmpty();
        assertThat(qualifiedFileIO.managedBlobDeleteAttempts()).isZero();
        for (Path pack : referenced) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @Test
    public void testReachabilityScanDeduplicatesReadsPerPass() throws Exception {
        FileStoreTable table = createManagedBlobTable("deduplicate_reachability_reads");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        write(
                table,
                GenericRow.of(2, BinaryString.fromString("b"), new BlobData(new byte[] {3, 4})));

        CountingInputFileIO countingFileIO = new CountingInputFileIO();
        FileStoreTable countingTable =
                FileStoreTableFactory.create(countingFileIO, table.location(), table.schema());
        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(
                        countingTable,
                        System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2),
                        false)) {
            cleaner.collectUsedPacks();
            assertReadOncePerPass(countingFileIO.readCounts());

            countingFileIO.reset();
            cleaner.collectUsedPacks();
            assertReadOncePerPass(countingFileIO.readCounts());
        }
    }

    @Test
    public void testSidecarWorkItemIsStableSerializableAndAddOnly() throws Exception {
        FileStoreTable table = createManagedBlobTable("sidecar_work_item");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        ManifestEntry add = table.store().newScan().plan().files().get(0);
        DataFilePathFactory pathFactory =
                new DataFilePathFactories(table.store().pathFactory())
                        .get(add.partition(), add.bucket());

        try (LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, true)) {
            List<ManagedBlobOrphanFilesClean.SidecarWorkItem> workItems =
                    cleaner.createSidecarWorkItems(add, pathFactory);
            assertThat(workItems).isNotEmpty();
            assertThat(cleaner.createSidecarWorkItems(add, pathFactory))
                    .extracting(ManagedBlobOrphanFilesClean.SidecarWorkItem::dedupIdentity)
                    .containsExactlyElementsOf(
                            workItems.stream()
                                    .map(ManagedBlobOrphanFilesClean.SidecarWorkItem::dedupIdentity)
                                    .collect(java.util.stream.Collectors.toList()));

            ManagedBlobOrphanFilesClean.SidecarWorkItem workItem = workItems.get(0);
            ManagedBlobOrphanFilesClean.SidecarWorkItem restored =
                    InstantiationUtil.clone(workItem);
            assertThat(restored).isEqualTo(workItem);
            assertThat(restored.dataFile()).isEqualTo(workItem.dataFile());
            assertThat(restored.sidecar()).isEqualTo(workItem.sidecar());
            assertThat(restored.extraFile()).isEqualTo(workItem.extraFile());

            ManifestEntry delete =
                    ManifestEntry.create(
                            FileKind.DELETE,
                            add.partition(),
                            add.bucket(),
                            add.totalBuckets(),
                            add.file());
            assertThat(cleaner.createSidecarWorkItems(delete, pathFactory)).isEmpty();
        }
    }

    @Test
    public void testExecuteDatabaseClosesExecutorsAfterTaskFailure() throws Exception {
        FileStoreTable table = createManagedBlobTable("database_failure_cleanup");
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondInterrupted = new CountDownLatch(1);
        AtomicBoolean firstClosed = new AtomicBoolean();
        AtomicBoolean secondClosed = new AtomicBoolean();
        LocalManagedBlobOrphanFilesClean first =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        try {
                            if (!secondStarted.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Second cleanup did not start.");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        throw new IOException("Expected cleanup failure.");
                    }

                    @Override
                    public void close() {
                        super.close();
                        firstClosed.set(true);
                    }
                };
        LocalManagedBlobOrphanFilesClean second =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        secondStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                            throw new IOException("Unexpected completion.");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            secondInterrupted.countDown();
                            throw new IOException(e);
                        }
                    }

                    @Override
                    public void close() {
                        super.close();
                        secondClosed.set(true);
                    }
                };
        ExecutorService databaseExecutor = Executors.newFixedThreadPool(2);

        Throwable failure =
                catchThrowable(
                        () ->
                                LocalManagedBlobOrphanFilesClean.executeDatabase(
                                        java.util.Arrays.asList(first, second), databaseExecutor));

        assertThat(failure).isInstanceOf(RuntimeException.class);
        assertThat(databaseExecutor.isShutdown()).isTrue();
        assertThat(firstClosed).isTrue();
        assertThat(secondClosed).isTrue();
        assertThat(secondInterrupted.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testExecuteDatabaseObservesLaterFailureBeforeEarlierTaskCompletes()
            throws Exception {
        FileStoreTable table = createManagedBlobTable("database_completion_order");
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch blockedInterrupted = new CountDownLatch(1);
        LocalManagedBlobOrphanFilesClean blocked =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        blockedStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                            throw new IOException("Unexpected completion.");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            blockedInterrupted.countDown();
                            throw new IOException(e);
                        }
                    }
                };
        LocalManagedBlobOrphanFilesClean failing =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        try {
                            if (!blockedStarted.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Earlier cleanup did not start.");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        throw new IOException("Expected later cleanup failure.");
                    }
                };
        ExecutorService databaseExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch executeReturned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller =
                new Thread(
                        () -> {
                            failure.set(
                                    catchThrowable(
                                            () ->
                                                    LocalManagedBlobOrphanFilesClean
                                                            .executeDatabase(
                                                                    java.util.Arrays.asList(
                                                                            blocked, failing),
                                                                    databaseExecutor)));
                            executeReturned.countDown();
                        });
        caller.start();

        boolean returnedInTime = executeReturned.await(10, TimeUnit.SECONDS);
        if (!returnedInTime) {
            caller.interrupt();
        }
        caller.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(returnedInTime).isTrue();
        assertThat(failure.get()).isInstanceOf(RuntimeException.class);
        assertThat(blockedInterrupted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(databaseExecutor.isTerminated()).isTrue();
    }

    @Test
    public void testExecuteDatabasePreservesInterruptAndClosesExecutors() throws Exception {
        FileStoreTable table = createManagedBlobTable("database_interrupt_cleanup");
        CountDownLatch cleanStarted = new CountDownLatch(1);
        CountDownLatch cleanInterrupted = new CountDownLatch(1);
        AtomicBoolean cleanerClosed = new AtomicBoolean();
        AtomicBoolean callerInterrupted = new AtomicBoolean();
        LocalManagedBlobOrphanFilesClean cleaner =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        cleanStarted.countDown();
                        try {
                            new CountDownLatch(1).await();
                            throw new IOException("Unexpected completion.");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            cleanInterrupted.countDown();
                            throw new IOException(e);
                        }
                    }

                    @Override
                    public void close() {
                        super.close();
                        cleanerClosed.set(true);
                    }
                };
        ExecutorService databaseExecutor = Executors.newSingleThreadExecutor();
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                LocalManagedBlobOrphanFilesClean.executeDatabase(
                                        java.util.Collections.singletonList(cleaner),
                                        databaseExecutor);
                            } catch (RuntimeException e) {
                                callerInterrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        caller.start();
        assertThat(cleanStarted.await(10, TimeUnit.SECONDS)).isTrue();

        caller.interrupt();
        caller.join(TimeUnit.SECONDS.toMillis(10));

        assertThat(caller.isAlive()).isFalse();
        assertThat(callerInterrupted).isTrue();
        assertThat(databaseExecutor.isShutdown()).isTrue();
        assertThat(cleanerClosed).isTrue();
        assertThat(cleanInterrupted.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testExecuteDatabaseWaitsForInterruptedDeleteLoop() throws Exception {
        FileStoreTable table = createManagedBlobTable("database_delete_loop_cleanup");
        Path firstOrphan = new Path(bucketPath(table), "first.managed.blob");
        Path secondOrphan = new Path(bucketPath(table), "second.managed.blob");
        table.fileIO().mkdirs(firstOrphan.getParent());
        table.fileIO().newOutputStream(firstOrphan, false).close();
        table.fileIO().newOutputStream(secondOrphan, false).close();

        CountDownLatch firstDeleteStarted = new CountDownLatch(1);
        CountDownLatch releaseUnexpectedSecondDelete = new CountDownLatch(1);
        AtomicInteger deleteAttempts = new AtomicInteger();
        FileIO interruptibleDeleteFileIO =
                new LocalFileIO() {
                    @Override
                    public boolean delete(Path path, boolean recursive) throws IOException {
                        if (!path.getName()
                                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                            return super.delete(path, recursive);
                        }
                        int attempt = deleteAttempts.incrementAndGet();
                        if (attempt == 1) {
                            firstDeleteStarted.countDown();
                            try {
                                new CountDownLatch(1).await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return false;
                        }
                        while (releaseUnexpectedSecondDelete.getCount() > 0) {
                            try {
                                releaseUnexpectedSecondDelete.await();
                            } catch (InterruptedException ignored) {
                                // Keep the unexpected delete observable until the assertion.
                            }
                        }
                        return false;
                    }
                };
        FileStoreTable interruptibleTable =
                FileStoreTableFactory.create(
                        interruptibleDeleteFileIO, table.location(), table.schema());
        AtomicBoolean actualCleanerReturned = new AtomicBoolean();
        LocalManagedBlobOrphanFilesClean actualCleaner =
                new LocalManagedBlobOrphanFilesClean(interruptibleTable, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        try {
                            return super.clean();
                        } finally {
                            actualCleanerReturned.set(true);
                        }
                    }
                };
        LocalManagedBlobOrphanFilesClean failingCleaner =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        try {
                            if (!firstDeleteStarted.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Managed blob deletion did not start.");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        throw new IOException("Expected cleanup failure.");
                    }
                };
        ExecutorService databaseExecutor = Executors.newFixedThreadPool(2);

        try {
            Throwable failure =
                    catchThrowable(
                            () ->
                                    LocalManagedBlobOrphanFilesClean.executeDatabase(
                                            java.util.Arrays.asList(failingCleaner, actualCleaner),
                                            databaseExecutor));

            assertThat(failure).isInstanceOf(RuntimeException.class);
            assertThat(actualCleanerReturned).isTrue();
            assertThat(deleteAttempts).hasValue(1);
            assertThat(databaseExecutor.isTerminated()).isTrue();
            assertThat(table.fileIO().exists(firstOrphan)).isTrue();
            assertThat(table.fileIO().exists(secondOrphan)).isTrue();
        } finally {
            releaseUnexpectedSecondDelete.countDown();
        }
    }

    @Test
    public void testExecuteDatabaseDoesNotHangOnUninterruptibleDelete() throws Exception {
        FileStoreTable table = createManagedBlobTable("database_uninterruptible_delete");
        Path firstOrphan = new Path(bucketPath(table), "first.managed.blob");
        Path secondOrphan = new Path(bucketPath(table), "second.managed.blob");
        table.fileIO().mkdirs(firstOrphan.getParent());
        table.fileIO().newOutputStream(firstOrphan, false).close();
        table.fileIO().newOutputStream(secondOrphan, false).close();

        CountDownLatch firstDeleteStarted = new CountDownLatch(1);
        AtomicBoolean releaseStuckDelete = new AtomicBoolean();
        AtomicInteger deleteAttempts = new AtomicInteger();
        FileIO uninterruptibleDeleteFileIO =
                new LocalFileIO() {
                    @Override
                    public boolean delete(Path path, boolean recursive) throws IOException {
                        if (!path.getName()
                                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                            return super.delete(path, recursive);
                        }
                        int attempt = deleteAttempts.incrementAndGet();
                        if (attempt == 1) {
                            firstDeleteStarted.countDown();
                            boolean interrupted = false;
                            while (!releaseStuckDelete.get()) {
                                try {
                                    Thread.sleep(20);
                                } catch (InterruptedException e) {
                                    interrupted = true;
                                }
                            }
                            if (interrupted) {
                                Thread.currentThread().interrupt();
                            }
                            return false;
                        }
                        return false;
                    }
                };
        FileStoreTable uninterruptibleTable =
                FileStoreTableFactory.create(
                        uninterruptibleDeleteFileIO, table.location(), table.schema());
        LocalManagedBlobOrphanFilesClean stuckCleaner =
                new LocalManagedBlobOrphanFilesClean(uninterruptibleTable, Long.MAX_VALUE, false);
        LocalManagedBlobOrphanFilesClean failingCleaner =
                new LocalManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false) {
                    @Override
                    public CleanOrphanFilesResult clean() throws IOException {
                        try {
                            if (!firstDeleteStarted.await(10, TimeUnit.SECONDS)) {
                                throw new IOException("Managed blob deletion did not start.");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException(e);
                        }
                        throw new IOException("Expected cleanup failure.");
                    }
                };
        ExecutorService databaseExecutor = Executors.newFixedThreadPool(2);
        CountDownLatch executeReturned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller =
                new Thread(
                        () -> {
                            failure.set(
                                    catchThrowable(
                                            () ->
                                                    LocalManagedBlobOrphanFilesClean
                                                            .executeDatabase(
                                                                    java.util.Arrays.asList(
                                                                            failingCleaner,
                                                                            stuckCleaner),
                                                                    databaseExecutor,
                                                                    200L)));
                            executeReturned.countDown();
                        });
        caller.start();

        try {
            assertThat(executeReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get())
                    .isInstanceOf(RuntimeException.class)
                    .hasRootCauseMessage("Expected cleanup failure.");
            assertThat(deleteAttempts).hasValue(1);
            assertThat(table.fileIO().exists(firstOrphan)).isTrue();
            assertThat(table.fileIO().exists(secondOrphan)).isTrue();
            assertThat(databaseExecutor.isTerminated()).isFalse();
        } finally {
            releaseStuckDelete.set(true);
            caller.join(TimeUnit.SECONDS.toMillis(10));
            databaseExecutor.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(deleteAttempts).hasValue(1);
        assertThat(table.fileIO().exists(firstOrphan)).isTrue();
        assertThat(table.fileIO().exists(secondOrphan)).isTrue();
    }

    @Test
    public void testEmptySidecarDoesNotBlockOthers() throws Exception {
        FileStoreTable table = createManagedBlobTable("empty_sidecar");
        write(table, GenericRow.of(1, BinaryString.fromString("a"), null));

        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);
        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
    }

    @Test
    public void testMissingSidecarSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("missing_sidecar");
        write(table, GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        deleteSidecars(table);
        List<Path> referenced = managedBlobs(table);
        referenced.remove(orphan);

        clean(table);

        assertThat(table.fileIO().exists(orphan)).isTrue();
        for (Path pack : referenced) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @Test
    public void testCorruptSidecarSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("corrupt_sidecar");
        write(table, GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        overwriteSidecars(
                table,
                out -> {
                    out.writeInt(0x50424C52);
                    out.writeByte(1);
                    out.writeInt(0);
                    out.writeInt(12345);
                });

        clean(table);
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testUnsupportedVersionSkipsAllPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("unsupported_sidecar");
        write(table, GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        overwriteSidecars(
                table,
                out -> {
                    out.writeInt(0x50424C52);
                    out.writeByte(99);
                    out.writeInt(0);
                });

        clean(table);
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testUnreferencedAfterUpdateAndExpire() throws Exception {
        FileStoreTable table = createManagedBlobTable("update_expire");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("old"), new BlobData(new byte[] {1, 1})));
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("new"), new BlobData(new byte[] {2, 2})));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Map<String, String> expire = new HashMap<>();
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(), "10");
        try (org.apache.paimon.table.sink.TableCommitImpl commit =
                table.copy(expire).newCommit("")) {
            commit.expireSnapshots();
        }

        Set<String> live = livePackNames(table);
        assertThat(live).isNotEmpty();
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted = clean(table);

        assertThat(table.fileIO().exists(orphan)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains("orphan.managed.blob");
        for (Path pack : managedBlobs(table)) {
            assertThat(live).contains(pack.getName());
        }
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 2, (byte) 2);
    }

    /**
     * Compaction can commit after orphan GC has listed snapshots. Expire then deletes
     * compact-before data files and blobrefs while those snapshots' manifests are still readable. A
     * used-file scan of the stale list therefore neither skips nor retains the reused pack.
     * Production GC collects used packs twice and aborts when the used set or snapshot topology
     * changes; this test keeps the scan-only interleaving. A compaction prepared from inputs that
     * are later removed cannot fill the remaining window after the second collection because
     * conflict detection rejects its stale commit; see {@link
     * #testStaleCompactionCannotCommitAfterFinalMark()}.
     */
    @Test
    public void testStaleSnapshotListMissesReusedPackAfterCompactBeforeDeleted() throws Exception {
        FileStoreTable table = createManagedBlobTable("stale_list_compact");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("old"), new BlobData(new byte[] {3, 3})));
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("new"), new BlobData(new byte[] {4, 4})));
        List<Snapshot> listed = new ArrayList<>(table.snapshotManager().safelyGetAllSnapshots());
        assertThat(listed).isNotEmpty();

        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        Set<String> liveAfterCompact = livePackNames(table);
        assertThat(liveAfterCompact).isNotEmpty();

        List<Path> compactBefore =
                table.store()
                        .newSnapshotDeletion()
                        .planDeletedInDeltaManifest(
                                table.snapshotManager().latestSnapshot(), entry -> false);
        assertThat(compactBefore).isNotEmpty();
        for (Path path : compactBefore) {
            table.fileIO().deleteQuietly(path);
        }

        StaleScan stale = collectUsedPacks(table, listed);
        assertThat(stale.skip).isFalse();
        assertThat(stale.packs).doesNotContainAnyElementsOf(liveAfterCompact);
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 4, (byte) 4);
    }

    @Test
    public void testAbortWhenUsedSetChangesBetweenCollections() throws Exception {
        FileStoreTable table = createManagedBlobTable("abort_used_change");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("old"), new BlobData(new byte[] {3, 3})));
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("new"), new BlobData(new byte[] {4, 4})));
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        List<Path> deleted =
                new LocalManagedBlobOrphanFilesClean(
                        table, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2), false) {
                    @Override
                    protected void betweenUsedCollections() {
                        try {
                            compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
                            List<Path> compactBefore =
                                    table.store()
                                            .newSnapshotDeletion()
                                            .planDeletedInDeltaManifest(
                                                    table.snapshotManager().latestSnapshot(),
                                                    entry -> false);
                            for (Path path : compactBefore) {
                                table.fileIO().deleteQuietly(path);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }.clean().getDeletedFilesPath();

        assertThat(deleted).isEmpty();
        assertThat(table.fileIO().exists(orphan)).isTrue();
        Set<String> live = livePackNames(table);
        assertThat(live).isNotEmpty();
        for (String name : live) {
            assertThat(table.fileIO().exists(new Path(bucketPath(table), name))).isTrue();
        }
        assertThat(read(table)).hasSize(1);
        assertThat(read(table).get(0).getBlob(2).toData()).containsExactly((byte) 4, (byte) 4);
    }

    @Test
    public void testStaleCompactionCannotCommitAfterFinalMark() throws Exception {
        FileStoreTable table = createManagedBlobTable("stale_compaction_commit");
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("old-1"), new BlobData(new byte[] {1, 1})));
        write(
                table,
                GenericRow.of(
                        2, BinaryString.fromString("old-2"), new BlobData(new byte[] {2, 2})));
        Set<String> oldPacks = livePackNames(table);
        assertThat(oldPacks).isNotEmpty();

        BatchWriteBuilder staleBuilder = table.newBatchWriteBuilder();
        List<CommitMessage> staleMessages;
        try (BatchTableWrite staleWrite = staleBuilder.newWrite()) {
            staleWrite.withIOManager(ioManager);
            staleWrite.compact(BinaryRow.EMPTY_ROW, 0, true);
            staleMessages = staleWrite.prepareCommit();
        }
        assertThat(staleMessages).isNotEmpty();
        Set<String> reusedPacks = compactAfterPackNames(table, staleMessages);
        assertThat(reusedPacks).isNotEmpty();
        assertThat(oldPacks).containsAll(reusedPacks);

        write(
                table,
                GenericRow.of(1, BinaryString.fromString("new-1"), new BlobData(new byte[] {3, 3})),
                GenericRow.of(
                        2, BinaryString.fromString("new-2"), new BlobData(new byte[] {4, 4})));
        compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Map<String, String> expire = new HashMap<>();
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expire.put(CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key(), "10");
        try (org.apache.paimon.table.sink.TableCommitImpl commit =
                table.copy(expire).newCommit("")) {
            commit.expireSnapshots();
        }

        Set<String> currentPacks = livePackNames(table);
        assertThat(currentPacks).isNotEmpty();
        assertThat(currentPacks).doesNotContainAnyElementsOf(oldPacks);

        AtomicBoolean commitAttempted = new AtomicBoolean();
        AtomicReference<Throwable> commitFailure = new AtomicReference<>();
        long snapshotIdBeforeClean = table.snapshotManager().latestSnapshotId();
        try (BatchTableCommit staleCommit = staleBuilder.newCommit()) {
            List<Path> deleted =
                    new LocalManagedBlobOrphanFilesClean(
                            table,
                            System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2),
                            false) {
                        @Override
                        protected boolean cleanManagedBlobFile(Path path) {
                            if (commitAttempted.compareAndSet(false, true)) {
                                commitFailure.set(
                                        catchThrowable(() -> staleCommit.commit(staleMessages)));
                            }
                            return super.cleanManagedBlobFile(path);
                        }
                    }.clean().getDeletedFilesPath();

            assertThat(commitAttempted).isTrue();
            assertThat(commitFailure.get())
                    .isNotNull()
                    .hasStackTraceContaining("File deletion conflicts detected");
            assertThat(deleted).extracting(Path::getName).containsAll(reusedPacks);
        }

        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshotIdBeforeClean);
        for (String pack : currentPacks) {
            assertThat(table.fileIO().exists(new Path(bucketPath(table), pack))).isTrue();
        }
        assertThat(read(table))
                .extracting(row -> row.getString(1).toString())
                .containsExactlyInAnyOrder("new-1", "new-2");
    }

    @Test
    public void testSuccessfulCompactionAfterFinalMarkKeepsReusedPacks() throws Exception {
        FileStoreTable table = createManagedBlobTable("successful_compaction_after_mark");
        write(
                table,
                GenericRow.of(
                        1, BinaryString.fromString("value-1"), new BlobData(new byte[] {1, 1})));
        write(
                table,
                GenericRow.of(
                        2, BinaryString.fromString("value-2"), new BlobData(new byte[] {2, 2})));
        Set<String> liveBeforeCompact = livePackNames(table);
        assertThat(liveBeforeCompact).isNotEmpty();

        Path orphan = new Path(bucketPath(table), "orphan-after-final-mark.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();

        AtomicBoolean compacted = new AtomicBoolean();
        long snapshotIdBeforeClean = table.snapshotManager().latestSnapshotId();
        List<Path> deleted =
                new LocalManagedBlobOrphanFilesClean(
                        table, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2), false) {
                    @Override
                    protected boolean cleanManagedBlobFile(Path path) {
                        if (compacted.compareAndSet(false, true)) {
                            try {
                                compact(table, BinaryRow.EMPTY_ROW, 0, ioManager, true);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return super.cleanManagedBlobFile(path);
                    }
                }.clean().getDeletedFilesPath();

        assertThat(compacted).isTrue();
        assertThat(table.snapshotManager().latestSnapshotId()).isGreaterThan(snapshotIdBeforeClean);
        assertThat(deleted).extracting(Path::getName).containsExactly(orphan.getName());

        Set<String> liveAfterCompact = livePackNames(table);
        assertThat(liveAfterCompact).isNotEmpty();
        assertThat(liveBeforeCompact).containsAll(liveAfterCompact);
        for (String pack : liveAfterCompact) {
            assertThat(table.fileIO().exists(new Path(bucketPath(table), pack))).isTrue();
        }
        assertThat(read(table))
                .extracting(row -> row.getString(1).toString())
                .containsExactlyInAnyOrder("value-1", "value-2");
    }

    @Test
    public void testJoinByFullPackPath() throws Exception {
        FileStoreTable table = createManagedBlobTable("full_path_join");
        write(
                table,
                GenericRow.of(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        List<Path> live = managedBlobs(table);
        assertThat(live).isNotEmpty();
        String liveName = live.get(0).getName();
        Path otherBucket = new Path(bucketPath(table).getParent(), "bucket-1");
        Path other = new Path(otherBucket, liveName);
        table.fileIO().mkdirs(otherBucket);
        table.fileIO().newOutputStream(other, false).close();

        List<Path> deleted = clean(table);

        assertThat(table.fileIO().exists(other)).isFalse();
        assertThat(deleted).extracting(Path::getName).contains(liveName);
        for (Path pack : live) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    private FileStoreTable createManagedBlobTable(String name) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("payload", DataTypes.BLOB())
                        .primaryKey("id")
                        .option(CoreOptions.BLOB_FIELD.key(), "payload")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "none")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(identifier(name), schema, true);
        return getTable(identifier(name));
    }

    private static List<Path> clean(FileStoreTable table) throws Exception {
        return new LocalManagedBlobOrphanFilesClean(
                        table, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2), false)
                .clean()
                .getDeletedFilesPath();
    }

    private static Path bucketPath(FileStoreTable table) {
        return table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
    }

    private static Set<String> livePackNames(FileStoreTable table) throws IOException {
        Set<String> names = new HashSet<>();
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (!extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    continue;
                }
                Path sidecar = pathFactory.toAlignedPath(extra, file);
                for (ManagedBlobReferenceFile.Reference ref :
                        ManagedBlobReferenceFile.read(fileIO, sidecar)) {
                    names.add(ref.relativePath());
                }
            }
        }
        return names;
    }

    private static List<Path> managedBlobs(FileStoreTable table) throws IOException {
        List<Path> packs = new ArrayList<>();
        FileStatus[] statuses = table.fileIO().listStatus(bucketPath(table));
        if (statuses == null) {
            return packs;
        }
        for (FileStatus status : statuses) {
            if (status.getPath().getName().endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                packs.add(status.getPath());
            }
        }
        return packs;
    }

    private static Set<String> compactAfterPackNames(
            FileStoreTable table, List<CommitMessage> messages) throws IOException {
        Set<String> packs = new HashSet<>();
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (CommitMessage message : messages) {
            CommitMessageImpl messageImpl = (CommitMessageImpl) message;
            DataFilePathFactory pathFactory = factories.get(message.partition(), message.bucket());
            for (DataFileMeta file : messageImpl.compactIncrement().compactAfter()) {
                for (String extra : file.extraFiles()) {
                    if (!extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                        continue;
                    }
                    Path sidecar = pathFactory.toAlignedPath(extra, file);
                    for (Reference reference : ManagedBlobReferenceFile.read(fileIO, sidecar)) {
                        packs.add(reference.relativePath());
                    }
                }
            }
        }
        return packs;
    }

    private static StaleScan collectUsedPacks(FileStoreTable table, Iterable<Snapshot> snapshots)
            throws IOException {
        StaleScan scan = new StaleScan();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        ManifestList manifestList = table.store().manifestListFactory().create();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        ManagedBlobReachabilityCollector collector =
                new ManagedBlobReachabilityCollector(table.fileIO());
        for (Snapshot snapshot : snapshots) {
            List<ManifestFileMeta> metas;
            try {
                metas = manifestList.readDataManifests(snapshot);
            } catch (Exception e) {
                scan.skip = true;
                return scan;
            }
            for (ManifestFileMeta meta : metas) {
                List<ManifestEntry> entries;
                try {
                    entries = manifestFile.read(meta.fileName());
                } catch (Exception e) {
                    scan.skip = true;
                    return scan;
                }
                for (ManifestEntry entry : entries) {
                    if (entry.kind() != FileKind.ADD) {
                        continue;
                    }
                    Result result =
                            collector.fromDataFile(
                                    factories.get(entry.partition(), entry.bucket()).toPath(entry),
                                    entry.file().extraFiles());
                    if (result.isUnsafe()) {
                        scan.skip = true;
                        return scan;
                    }
                    for (Reference reference : result.referenced()) {
                        scan.packs.add(reference.relativePath());
                    }
                }
            }
        }
        return scan;
    }

    private static final class StaleScan {
        private boolean skip;
        private final Set<String> packs = new HashSet<>();
    }

    private static void deleteSidecars(FileStoreTable table) throws IOException {
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    fileIO.deleteQuietly(pathFactory.toAlignedPath(extra, file));
                }
            }
        }
    }

    private static void assertReadOncePerPass(Map<String, Integer> readCounts) {
        assertThat(readCounts)
                .anySatisfy(
                        (path, count) -> {
                            assertThat(path)
                                    .endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX);
                            assertThat(count).isEqualTo(1);
                        });
        assertThat(readCounts)
                .anySatisfy(
                        (path, count) -> {
                            assertThat(new Path(path).getName())
                                    .startsWith("manifest-")
                                    .doesNotStartWith("manifest-list-");
                            assertThat(count).isEqualTo(1);
                        });
        assertThat(readCounts.values()).allMatch(count -> count == 1);
    }

    private static class RelativeListingFileIO extends TraceableFileIO {

        private final AtomicInteger managedBlobDeleteAttempts = new AtomicInteger();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            if (statuses == null) {
                return null;
            }
            FileStatus[] relative = new FileStatus[statuses.length];
            for (int i = 0; i < statuses.length; i++) {
                FileStatus status = statuses[i];
                relative[i] =
                        status.getPath()
                                        .getName()
                                        .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)
                                ? withPath(status, toRelativePath(status.getPath()))
                                : status;
            }
            return relative;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            FileStatus status = super.getFileStatus(path);
            return path.toUri().getPath().startsWith("/") ? status : withPath(status, path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (path.getName().endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                managedBlobDeleteAttempts.incrementAndGet();
            }
            return super.delete(path, recursive);
        }

        private int managedBlobDeleteAttempts() {
            return managedBlobDeleteAttempts.get();
        }

        private static Path toRelativePath(Path path) {
            java.nio.file.Path absolute = Paths.get(path.toUri().getPath()).toAbsolutePath();
            return new Path(Paths.get("").toAbsolutePath().relativize(absolute).toString());
        }

        private static FileStatus withPath(FileStatus status, Path path) {
            return new FileStatus() {
                @Override
                public long getLen() {
                    return status.getLen();
                }

                @Override
                public boolean isDir() {
                    return status.isDir();
                }

                @Override
                public Path getPath() {
                    return path;
                }

                @Override
                public long getModificationTime() {
                    return status.getModificationTime();
                }
            };
        }
    }

    private static class QualifiedListingFileIO extends LocalFileIO {

        private final AtomicInteger managedBlobDeleteAttempts = new AtomicInteger();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            if (statuses == null) {
                return null;
            }
            FileStatus[] qualified = new FileStatus[statuses.length];
            for (int i = 0; i < statuses.length; i++) {
                FileStatus status = statuses[i];
                if (!status.getPath()
                        .getName()
                        .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                    qualified[i] = status;
                    continue;
                }
                qualified[i] =
                        new FileStatus() {
                            @Override
                            public long getLen() {
                                return status.getLen();
                            }

                            @Override
                            public boolean isDir() {
                                return status.isDir();
                            }

                            @Override
                            public Path getPath() {
                                return new Path(
                                        "hdfs://namenode:8020"
                                                + status.getPath().toUri().getPath());
                            }

                            @Override
                            public long getModificationTime() {
                                return status.getModificationTime();
                            }
                        };
            }
            return qualified;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (path.getName().endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                managedBlobDeleteAttempts.incrementAndGet();
                return super.delete(new Path(path.toUri().getPath()), recursive);
            }
            return super.delete(path, recursive);
        }

        private int managedBlobDeleteAttempts() {
            return managedBlobDeleteAttempts.get();
        }
    }

    private static class CountingInputFileIO extends LocalFileIO {

        private final Map<String, AtomicInteger> readCounts = new ConcurrentHashMap<>();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String fileName = path.getName();
            if (fileName.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)
                    || (fileName.startsWith("manifest-")
                            && !fileName.startsWith("manifest-list-"))) {
                readCounts
                        .computeIfAbsent(path.toUri().getPath(), ignored -> new AtomicInteger())
                        .incrementAndGet();
            }
            return super.newInputStream(path);
        }

        private Map<String, Integer> readCounts() {
            Map<String, Integer> result = new HashMap<>();
            readCounts.forEach((path, count) -> result.put(path, count.get()));
            return result;
        }

        private void reset() {
            readCounts.clear();
        }
    }

    private interface SidecarOverwriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static void overwriteSidecars(FileStoreTable table, SidecarOverwriter overwriter)
            throws IOException {
        FileIO fileIO = table.fileIO();
        DataFilePathFactories factories = new DataFilePathFactories(table.store().pathFactory());
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            DataFilePathFactory pathFactory = factories.get(entry.partition(), entry.bucket());
            DataFileMeta file = entry.file();
            for (String extra : file.extraFiles()) {
                if (!extra.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                    continue;
                }
                Path sidecar = pathFactory.toAlignedPath(extra, file);
                fileIO.deleteQuietly(sidecar);
                try (DataOutputStream out =
                        new DataOutputStream(fileIO.newOutputStream(sidecar, false))) {
                    overwriter.write(out);
                }
            }
        }
    }
}
