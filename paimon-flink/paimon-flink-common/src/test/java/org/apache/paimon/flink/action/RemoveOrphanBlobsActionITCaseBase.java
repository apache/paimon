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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.flink.orphan.FlinkManagedBlobOrphanFilesClean;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.CleanOrphanFilesResult;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link RemoveOrphanBlobsAction}. */
public abstract class RemoveOrphanBlobsActionITCaseBase extends ActionITCaseBase {

    private static final AtomicBoolean FAIL_AFTER_FIRST_DELETE = new AtomicBoolean();
    private static final List<CleanOrphanFilesResult> FAILOVER_CLEANUP_RESULTS =
            Collections.synchronizedList(new ArrayList<CleanOrphanFilesResult>());

    @ParameterizedTest
    @ValueSource(strings = {"local", "distributed"})
    public void testDeleteUnreferencedManagedBlobPack(String mode) throws Exception {
        FileStoreTable table = createManagedBlobTableAndWrite();
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        Thread.sleep(2000);

        List<Path> referenced =
                filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        referenced.removeIf(p -> orphan.getName().equals(p.getName()));
        assertThat(referenced).isNotEmpty();

        ImmutableList.copyOf(executeSQL(removeOrphanBlobsCall(mode)));

        assertThat(table.fileIO().exists(orphan)).isFalse();
        for (Path pack : referenced) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"local", "distributed"})
    public void testMissingManagedBlobSidecarSkipsPackGc(String mode) throws Exception {
        FileStoreTable table = createManagedBlobTableAndWrite();
        Path orphanPack = new Path(bucketPath(table), "orphan.managed.blob");
        Path orphanOther = new Path(bucketPath(table), "orphan.txt");
        table.fileIO().newOutputStream(orphanPack, false).close();
        table.fileIO().writeFile(orphanOther, "x", true);
        Thread.sleep(2000);

        List<Path> referenced =
                filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        referenced.removeIf(p -> orphanPack.getName().equals(p.getName()));
        assertThat(referenced).isNotEmpty();
        deleteFilesWithSuffix(table, ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX);

        ImmutableList.copyOf(executeSQL(removeOrphanBlobsCall(mode)));

        assertThat(table.fileIO().exists(orphanPack)).isTrue();
        assertThat(table.fileIO().exists(orphanOther)).isTrue();
        for (Path pack : referenced) {
            assertThat(table.fileIO().exists(pack)).isTrue();
        }
    }

    @Test
    public void testDistributedDeleteWithEmptyUsedPackSet() throws Exception {
        FileStoreTable table = createManagedBlobTable();
        Path bucket = bucketPath(table);
        table.fileIO().mkdirs(bucket);
        Path orphan = new Path(bucket, "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        Thread.sleep(2000);

        ImmutableList.copyOf(executeSQL(removeOrphanBlobsCall("distributed")));

        assertThat(table.fileIO().exists(orphan)).isFalse();
    }

    @Test
    public void testDistributedUnresolvableRelativeCandidateSkipsGc() throws Exception {
        FileStoreTable table = createManagedBlobTableAndWrite();
        Path orphan = new Path(bucketPath(table), "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        Thread.sleep(2000);

        UnresolvableRelativeListingFileIO.reset();
        FileStoreTable relativeListingTable =
                FileStoreTableFactory.create(
                        new UnresolvableRelativeListingFileIO(), table.location(), table.schema());
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().batchMode().parallelism(5).build();
        FlinkManagedBlobOrphanFilesClean cleaner =
                new FlinkManagedBlobOrphanFilesClean(
                        relativeListingTable, Long.MAX_VALUE, false, 5);
        List<CleanOrphanFilesResult> cleanResults = new ArrayList<>();
        try (CloseableIterator<CleanOrphanFilesResult> results =
                cleaner.doClean(env).executeAndCollect()) {
            while (results.hasNext()) {
                cleanResults.add(results.next());
            }
        }

        assertThat(UnresolvableRelativeListingFileIO.resolveAttempts()).isPositive();
        assertThat(cleanResults).isNotEmpty();
        assertThat(cleanResults)
                .allSatisfy(
                        result -> {
                            assertThat(result.getDeletedFileCount()).isZero();
                            assertThat(result.getDeletedFileTotalLenInBytes()).isZero();
                        });
        assertThat(table.fileIO().exists(orphan)).isTrue();
    }

    @Test
    public void testDistributedMarkReadsSharedMetadataOncePerPass() throws Exception {
        FileStoreTable table = createManagedBlobTable();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
        writeData(rowData(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        writeData(rowData(2, BinaryString.fromString("b"), new BlobData(new byte[] {3, 4})));
        write.close();
        commit.close();
        write = null;
        commit = null;
        try (TableCommitImpl manifestCommit = table.newCommit("manifest-compact-test")) {
            manifestCommit.compactManifests();
        }
        assertSharedManifestMetadata(table);
        int parallelism = assertSharedSidecarAcrossManifestPartitions(table);

        CountingManifestFileIO.reset();
        FileStoreTable countingTable =
                FileStoreTableFactory.create(
                        new CountingManifestFileIO(), table.location(), table.schema());
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().batchMode().parallelism(parallelism).build();
        FlinkManagedBlobOrphanFilesClean cleaner =
                new FlinkManagedBlobOrphanFilesClean(
                        countingTable, Long.MAX_VALUE, true, parallelism);
        try (CloseableIterator<CleanOrphanFilesResult> results =
                cleaner.doClean(env).executeAndCollect()) {
            while (results.hasNext()) {
                results.next();
            }
        }

        assertThat(CountingManifestFileIO.readCounts()).isNotEmpty();
        assertThat(CountingManifestFileIO.readCounts().keySet())
                .anyMatch(path -> path.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX));
        assertThat(CountingManifestFileIO.readCounts().values()).allMatch(count -> count == 2);
    }

    @Test
    @Timeout(60)
    public void testDistributedCleanupDoesNotHangWithPipelinedShuffle() throws Exception {
        FileStoreTable table = createManagedBlobTableAndWrite();
        Path bucket = bucketPath(table);
        List<Path> referenced =
                filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        assertThat(referenced).isNotEmpty();
        for (int i = 0; i < 256; i++) {
            table.fileIO()
                    .newOutputStream(new Path(bucket, "orphan-" + i + ".managed.blob"), false)
                    .close();
        }
        Thread.sleep(2000);

        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .batchMode()
                        .parallelism(2)
                        .setConf(
                                ExecutionOptions.BATCH_SHUFFLE_MODE,
                                BatchShuffleMode.ALL_EXCHANGES_PIPELINED)
                        .build();
        FlinkManagedBlobOrphanFilesClean cleaner =
                new FlinkManagedBlobOrphanFilesClean(table, Long.MAX_VALUE, false, 2);
        DataStream<CleanOrphanFilesResult> clean = cleaner.doClean(env);
        assertThat(env.getConfiguration().get(ExecutionOptions.BATCH_SHUFFLE_MODE))
                .isEqualTo(BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        long deleted = 0;
        try (CloseableIterator<CleanOrphanFilesResult> results = clean.executeAndCollect()) {
            while (results.hasNext()) {
                deleted += results.next().getDeletedFileCount();
            }
        }
        assertThat(deleted).isEqualTo(256);
        assertThat(filesWithSuffix(table, ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX))
                .containsExactlyInAnyOrderElementsOf(referenced);
    }

    @Test
    public void testDistributedDeletionAccountingSurvivesCleanupTaskRestartAfterDelete()
            throws Exception {
        FileStoreTable table = createManagedBlobTable();
        Path bucket = bucketPath(table);
        table.fileIO().mkdirs(bucket);
        Path first = new Path(bucket, "first.managed.blob");
        Path second = new Path(bucket, "second.managed.blob");
        String firstContent = "one";
        String secondContent = "second";
        table.fileIO().writeFile(first, firstContent, false);
        table.fileIO().writeFile(second, secondContent, false);
        long expectedDeletedBytes = firstContent.length() + secondContent.length();
        Thread.sleep(2000);

        FAIL_AFTER_FIRST_DELETE.set(true);
        FAILOVER_CLEANUP_RESULTS.clear();
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder()
                        .batchMode()
                        .parallelism(1)
                        .allowRestart()
                        .build();
        FlinkManagedBlobOrphanFilesClean cleaner = new FailAfterFirstDeleteCleaner(table);

        // executeAndCollect() uses an uncheckpointed collect buffer that throws "Job restarted"
        // on failover, so collect operator output through a MiniCluster-local sink instead.
        cleaner.doClean(env).map(new CollectCleanupResult()).sinkTo(new DiscardingSink<>());
        env.execute();

        long deleted = 0;
        long deletedBytes = 0;
        for (CleanOrphanFilesResult result : FAILOVER_CLEANUP_RESULTS) {
            deleted += result.getDeletedFileCount();
            deletedBytes += result.getDeletedFileTotalLenInBytes();
        }

        assertThat(FAIL_AFTER_FIRST_DELETE).isFalse();
        assertThat(deleted).isEqualTo(2);
        assertThat(deletedBytes).isEqualTo(expectedDeletedBytes);
        assertThat(table.fileIO().exists(first)).isFalse();
        assertThat(table.fileIO().exists(second)).isFalse();
    }

    @Test
    public void testDistributedCanonicalAliasCandidatesAreCountedOnce() throws Exception {
        FileStoreTable table = createManagedBlobTable();
        Path bucket = bucketPath(table);
        table.fileIO().mkdirs(bucket);
        Path orphan = new Path(bucket, "duplicate.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        Thread.sleep(2000);

        FileStoreTable duplicateListingTable =
                FileStoreTableFactory.create(
                        new CanonicalAliasListingFileIO(), table.location(), table.schema());
        StreamExecutionEnvironment env =
                streamExecutionEnvironmentBuilder().batchMode().parallelism(2).build();
        FlinkManagedBlobOrphanFilesClean cleaner =
                new FlinkManagedBlobOrphanFilesClean(
                        duplicateListingTable, Long.MAX_VALUE, false, 2);

        long deleted = 0;
        try (CloseableIterator<CleanOrphanFilesResult> results =
                cleaner.doClean(env).executeAndCollect()) {
            while (results.hasNext()) {
                deleted += results.next().getDeletedFileCount();
            }
        }

        assertThat(deleted).isEqualTo(1);
        assertThat(table.fileIO().exists(orphan)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testActionFactoryDryRunParsing(boolean dryRun) throws Exception {
        FileStoreTable table = createManagedBlobTable();
        Path bucket = bucketPath(table);
        table.fileIO().mkdirs(bucket);
        Path orphan = new Path(bucket, "orphan.managed.blob");
        table.fileIO().newOutputStream(orphan, false).close();
        Thread.sleep(2000);

        createAction(
                        RemoveOrphanBlobsAction.class,
                        "remove_orphan_blobs",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--older_than",
                        currentTimestamp(),
                        "--dry_run",
                        String.valueOf(dryRun),
                        "--parallelism",
                        "2")
                .run();

        assertThat(table.fileIO().exists(orphan)).isEqualTo(dryRun);
    }

    @Test
    public void testActionFactoryRejectsInvalidDryRun() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        RemoveOrphanBlobsAction.class,
                                        "remove_orphan_blobs",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--dry_run",
                                        "ture"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Argument 'dry_run' must be either 'true' or 'false', but was 'ture'.");
    }

    @Test
    public void testActionFactoryRejectsNonPositiveParallelism() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        RemoveOrphanBlobsAction.class,
                                        "remove_orphan_blobs",
                                        "--warehouse",
                                        warehouse,
                                        "--database",
                                        database,
                                        "--table",
                                        tableName,
                                        "--parallelism",
                                        "0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Parallelism must be greater than 0, but was 0.");
    }

    @Test
    public void testProcedureRejectsNonPositiveParallelism() {
        String call =
                supportNamedArgument()
                        ? String.format(
                                "CALL sys.remove_orphan_blobs(`table` => '%s.%s', parallelism => 0)",
                                database, tableName)
                        : String.format(
                                "CALL sys.remove_orphan_blobs('%s.%s', '', false, 0)",
                                database, tableName);

        assertThatThrownBy(() -> ImmutableList.copyOf(executeSQL(call)))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Parallelism must be greater than 0, but was 0."));
    }

    private FileStoreTable createManagedBlobTableAndWrite() throws Exception {
        FileStoreTable table = createManagedBlobTable();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();
        writeData(rowData(1, BinaryString.fromString("a"), new BlobData(new byte[] {1, 2})));
        write.close();
        commit.close();
        write = null;
        commit = null;
        return table;
    }

    private FileStoreTable createManagedBlobTable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BLOB_FIELD.key(), "payload");
        options.put(CoreOptions.CHANGELOG_PRODUCER.key(), "none");
        options.put("bucket", "1");
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BLOB()},
                        new String[] {"id", "name", "payload"});
        return createFileStoreTable(
                tableName,
                rowType,
                Collections.emptyList(),
                Collections.singletonList("id"),
                Collections.emptyList(),
                options);
    }

    private String removeOrphanBlobsCall(String mode) {
        String olderThan = currentTimestamp();
        if (supportNamedArgument()) {
            return String.format(
                    "CALL sys.remove_orphan_blobs(`table` => '%s.%s', older_than => '%s', parallelism => 5, mode => '%s')",
                    database, tableName, olderThan, mode);
        }
        return String.format(
                "CALL sys.remove_orphan_blobs('%s.%s', '%s', false, 5, '%s')",
                database, tableName, olderThan, mode);
    }

    private static String currentTimestamp() {
        return DateTimeUtils.formatLocalDateTime(
                DateTimeUtils.toLocalDateTime(System.currentTimeMillis()), 3);
    }

    private static Path bucketPath(FileStoreTable table) {
        return table.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
    }

    private static List<Path> filesWithSuffix(FileStoreTable table, String suffix)
            throws IOException {
        List<Path> result = new ArrayList<>();
        FileStatus[] statuses = table.fileIO().listStatus(bucketPath(table));
        if (statuses == null) {
            return result;
        }
        for (FileStatus status : statuses) {
            if (status.getPath().getName().endsWith(suffix)) {
                result.add(status.getPath());
            }
        }
        return result;
    }

    private static void deleteFilesWithSuffix(FileStoreTable table, String suffix)
            throws IOException {
        for (Path path : filesWithSuffix(table, suffix)) {
            table.fileIO().deleteQuietly(path);
        }
    }

    private static void assertSharedManifestMetadata(FileStoreTable table) throws IOException {
        ManifestList manifestList = table.store().manifestListFactory().create();
        Map<String, Integer> references = new HashMap<>();
        Set<String> readLists = new HashSet<>();
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        while (snapshots.hasNext()) {
            Snapshot snapshot = snapshots.next();
            String[] listNames = {
                snapshot.changelogManifestList(),
                snapshot.deltaManifestList(),
                snapshot.baseManifestList()
            };
            for (String listName : listNames) {
                if (listName == null) {
                    continue;
                }
                references.merge("list:" + listName, 1, Integer::sum);
                if (readLists.add(listName)) {
                    for (ManifestFileMeta meta : manifestList.readWithIOException(listName)) {
                        references.merge("manifest:" + meta.fileName(), 1, Integer::sum);
                    }
                }
            }
        }
        assertThat(references.values()).anyMatch(count -> count > 1);
    }

    private static int assertSharedSidecarAcrossManifestPartitions(FileStoreTable table)
            throws IOException {
        ManifestList manifestList = table.store().manifestListFactory().create();
        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        DataFilePathFactories pathFactories =
                new DataFilePathFactories(table.store().pathFactory());
        Set<String> readLists = new HashSet<>();
        Set<String> readManifests = new HashSet<>();
        Map<String, Set<String>> sidecarManifests = new HashMap<>();
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        while (snapshots.hasNext()) {
            Snapshot snapshot = snapshots.next();
            String[] listNames = {
                snapshot.changelogManifestList(),
                snapshot.deltaManifestList(),
                snapshot.baseManifestList()
            };
            for (String listName : listNames) {
                if (listName == null || !readLists.add(listName)) {
                    continue;
                }
                for (ManifestFileMeta meta : manifestList.readWithIOException(listName)) {
                    if (!readManifests.add(meta.fileName())) {
                        continue;
                    }
                    for (ManifestEntry entry : manifestFile.readWithIOException(meta.fileName())) {
                        if (entry.kind() != FileKind.ADD || entry.file().extraFiles() == null) {
                            continue;
                        }
                        Path dataFile =
                                pathFactories.get(entry.partition(), entry.bucket()).toPath(entry);
                        for (String extraFile : entry.file().extraFiles()) {
                            if (extraFile != null
                                    && extraFile.endsWith(
                                            ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                                String sidecar =
                                        new Path(dataFile.getParent(), extraFile)
                                                .toUri()
                                                .normalize()
                                                .toString();
                                sidecarManifests
                                        .computeIfAbsent(sidecar, ignored -> new HashSet<>())
                                        .add(meta.fileName());
                            }
                        }
                    }
                }
            }
        }

        assertThat(sidecarManifests.values()).anyMatch(manifests -> manifests.size() > 1);
        for (int parallelism = 2; parallelism <= 16; parallelism++) {
            int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
            for (Set<String> manifests : sidecarManifests.values()) {
                Set<Integer> partitions = new HashSet<>();
                for (String manifest : manifests) {
                    partitions.add(
                            KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                    DEFAULT_MAIN_BRANCH + '\0' + manifest,
                                    maxParallelism,
                                    parallelism));
                }
                if (partitions.size() > 1) {
                    return parallelism;
                }
            }
        }
        throw new AssertionError(
                "Shared sidecars did not span manifest-reader partitions for parallelism 2-16.");
    }

    private static class CountingManifestFileIO extends LocalFileIO {

        private static final Map<String, AtomicInteger> READ_COUNTS = new ConcurrentHashMap<>();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            String name = path.getName();
            if (name.startsWith("manifest-list-")
                    || (name.startsWith("manifest-") && !name.startsWith("manifest-list-"))
                    || name.endsWith(ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX)) {
                READ_COUNTS
                        .computeIfAbsent(path.toUri().getPath(), ignored -> new AtomicInteger())
                        .incrementAndGet();
            }
            return super.newInputStream(path);
        }

        private static Map<String, Integer> readCounts() {
            Map<String, Integer> result = new HashMap<>();
            READ_COUNTS.forEach((path, count) -> result.put(path, count.get()));
            return result;
        }

        private static void reset() {
            READ_COUNTS.clear();
        }
    }

    private static class UnresolvableRelativeListingFileIO extends TraceableFileIO {

        private static final AtomicInteger RESOLVE_ATTEMPTS = new AtomicInteger();

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
                                ? withPath(status, new Path(status.getPath().getName()))
                                : status;
            }
            return relative;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            if (!path.toUri().getPath().startsWith("/")) {
                RESOLVE_ATTEMPTS.incrementAndGet();
                throw new IOException("Cannot resolve relative path " + path);
            }
            return super.getFileStatus(path);
        }

        private static int resolveAttempts() {
            return RESOLVE_ATTEMPTS.get();
        }

        private static void reset() {
            RESOLVE_ATTEMPTS.set(0);
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

    private static class CanonicalAliasListingFileIO extends TraceableFileIO {

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            if (statuses == null) {
                return null;
            }
            List<FileStatus> duplicated = new ArrayList<>();
            for (FileStatus status : statuses) {
                duplicated.add(status);
                if (status.getPath()
                        .getName()
                        .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX)) {
                    Path alias =
                            new Path(
                                    "hdfs://duplicate-listing"
                                            + status.getPath().toUri().getPath());
                    duplicated.add(UnresolvableRelativeListingFileIO.withPath(status, alias));
                }
            }
            return duplicated.toArray(new FileStatus[0]);
        }
    }

    private static class FailAfterFirstDeleteCleaner extends FlinkManagedBlobOrphanFilesClean {

        private FailAfterFirstDeleteCleaner(FileStoreTable table) {
            super(table, Long.MAX_VALUE, false, 1);
        }

        @Override
        protected boolean cleanPack(Path path) {
            boolean cleaned = super.cleanPack(path);
            if (cleaned && FAIL_AFTER_FIRST_DELETE.compareAndSet(true, false)) {
                throw new RuntimeException("Injected failure after delete.");
            }
            return cleaned;
        }
    }

    private static class CollectCleanupResult
            implements MapFunction<CleanOrphanFilesResult, CleanOrphanFilesResult> {

        @Override
        public CleanOrphanFilesResult map(CleanOrphanFilesResult value) {
            FAILOVER_CLEANUP_RESULTS.add(value);
            return value;
        }
    }

    protected boolean supportNamedArgument() {
        return true;
    }
}
