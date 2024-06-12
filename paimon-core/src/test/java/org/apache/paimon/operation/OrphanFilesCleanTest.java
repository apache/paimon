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

import org.apache.paimon.Changelog;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerStreamTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.StringUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.io.DataFilePathFactory.DATA_FILE_PREFIX;
import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OrphanFilesClean}. */
public class OrphanFilesCleanTest {

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    @TempDir private java.nio.file.Path tempDir;
    private Path tablePath;
    private FileIO fileIO;
    private RowType rowType;
    private FileStoreTable table;
    private TableWriteImpl<?> write;
    private TableCommitImpl commit;
    private Path manifestDir;

    private long incrementalIdentifier;
    private List<Path> manuallyAddedFiles;

    @BeforeEach
    public void beforeEach() throws Exception {
        tablePath = new Path(tempDir.toString());
        fileIO = LocalFileIO.create();
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()
                        },
                        new String[] {"pk", "part1", "part2", "value"});
        table = createFileStoreTable(rowType, new Options());
        String commitUser = UUID.randomUUID().toString();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        manifestDir = new Path(tablePath, "manifest");

        incrementalIdentifier = 0;
        manuallyAddedFiles = new ArrayList<>();
    }

    @AfterEach
    public void afterEach() throws Exception {
        write.close();
        commit.close();
        TestPojo.reset();
    }

    @Test
    public void testNormallyRemoving() throws Throwable {
        int commitTimes = 30;
        List<List<TestPojo>> committedData = new ArrayList<>();
        Map<Long, List<TestPojo>> snapshotData = new HashMap<>();

        SnapshotManager snapshotManager = table.snapshotManager();
        writeData(snapshotManager, committedData, snapshotData, new HashMap<>(), commitTimes);

        // randomly create tags
        List<String> allTags = new ArrayList<>();
        int snapshotCount = (int) snapshotManager.snapshotCount();
        for (int i = 1; i <= snapshotCount; i++) {
            if (RANDOM.nextBoolean()) {
                String tagName = "tag" + i;
                table.createTag(tagName, i);
                allTags.add(tagName);
            }
        }

        // generate non used files
        int shouldBeDeleted = generateUnUsedFile();
        assertThat(manuallyAddedFiles.size()).isEqualTo(shouldBeDeleted);

        // randomly expire snapshots
        int expired = RANDOM.nextInt(snapshotCount / 2);
        expired = expired == 0 ? 1 : expired;
        Options expireOptions = new Options();
        expireOptions.set(CoreOptions.SNAPSHOT_EXPIRE_LIMIT, snapshotCount);
        expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, snapshotCount - expired);
        expireOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, snapshotCount - expired);
        table.copy(expireOptions.toMap()).newCommit("").expireSnapshots();

        // randomly delete tags
        List<String> deleteTags = Collections.emptyList();
        if (!allTags.isEmpty()) {
            deleteTags = randomlyPick(allTags);
            for (String tagName : deleteTags) {
                table.deleteTag(tagName);
            }
        }

        // first check, nothing will be deleted because the default olderThan interval is 1 day
        OrphanFilesClean orphanFilesClean = new OrphanFilesClean(table);
        assertThat(orphanFilesClean.clean().size()).isEqualTo(0);

        // second check
        orphanFilesClean = new OrphanFilesClean(table);
        setOlderThan(orphanFilesClean);
        orphanFilesClean.clean();
        try {
            validate(orphanFilesClean.getDeleteFiles(), snapshotData, new HashMap<>());
        } catch (Throwable t) {
            String tableOptions = "Table options:\n" + table.options();

            String committed = "Committed data:";
            for (int i = 0; i < committedData.size(); i++) {
                String insertValues =
                        committedData.get(i).stream()
                                .map(TestPojo::toInsertValueString)
                                .collect(Collectors.joining(","));
                committed = String.format("%s\n%d:{%s}", committed, i, insertValues);
            }

            String snapshot = "Snapshot expired: " + expired;

            String tag =
                    String.format(
                            "Tags: created{%s}; deleted{%s}",
                            String.join(",", allTags), String.join(",", deleteTags));

            String addedFile =
                    "Manually added file:\n"
                            + manuallyAddedFiles.stream()
                                    .map(Path::toString)
                                    .collect(Collectors.joining("\n"));

            throw new Exception(
                    String.format(
                            "%s\n%s\n%s\n%s\n%s",
                            tableOptions, committed, snapshot, tag, addedFile),
                    t);
        }
    }

    private void validate(
            List<Path> deleteFiles,
            Map<Long, List<TestPojo>> snapshotData,
            Map<Long, List<InternalRow>> changelogData)
            throws Exception {
        assertThat(
                        deleteFiles.stream()
                                .map(p -> p.toUri().getPath())
                                .sorted()
                                .collect(Collectors.joining("\n")))
                .isEqualTo(
                        manuallyAddedFiles.stream()
                                .map(p -> p.toUri().getPath())
                                .sorted()
                                .collect(Collectors.joining("\n")));

        Set<Snapshot> snapshots = new HashSet<>();
        table.snapshotManager().snapshots().forEachRemaining(snapshots::add);
        snapshots.addAll(table.tagManager().taggedSnapshots());
        List<Snapshot> sorted =
                snapshots.stream()
                        .sorted(Comparator.comparingLong(Snapshot::id))
                        .collect(Collectors.toList());

        for (Snapshot snapshot : sorted) {
            try {
                validateSnapshot(snapshot, snapshotData.get(snapshot.id()));
            } catch (Exception e) {
                throw new Exception("Failed to validate snapshot " + snapshot.id(), e);
            }
        }
        // validate changelog
        if (table.coreOptions().changelogProducer() == CoreOptions.ChangelogProducer.INPUT) {
            List<Changelog> changelogs = new ArrayList<>();
            table.snapshotManager().changelogs().forEachRemaining(changelogs::add);
            validateChangelog(
                    changelogs.stream()
                            .sorted(Comparator.comparingLong(Changelog::id))
                            .collect(Collectors.toList()),
                    changelogData);
        }
    }

    private void validateChangelog(
            List<Changelog> changelogs, Map<Long, List<InternalRow>> changelogData)
            throws Exception {
        Preconditions.checkArgument(!changelogs.isEmpty(), "The changelogs should not be empty!");
        FileStoreTable scanTable =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                String.valueOf(changelogs.get(0).id())));
        Long max =
                changelogData.keySet().stream()
                        .max(Comparator.comparingLong(Long::longValue))
                        .get();
        InnerStreamTableScan scan = scanTable.newStreamScan();
        TreeMap<Long, List<InternalRow>> data = new TreeMap<>(changelogData);
        // clear the data < the smallest changelog data.
        data.headMap(changelogs.get(0).id()).clear();

        // initial plan
        scan.plan();
        Long id = changelogs.get(0).id();
        while (id <= max) {
            List<Split> splits = scan.plan().splits();
            if (!splits.isEmpty()) {
                List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
                for (Split split : splits) {
                    readers.add(() -> scanTable.newRead().createReader(split));
                }
                RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
                RecordReaderIterator<InternalRow> iterator =
                        new RecordReaderIterator<>(recordReader);
                List<String> result = new ArrayList<>();
                while (iterator.hasNext()) {
                    InternalRow rowData = iterator.next();
                    result.add(DataFormatTestUtil.internalRowToString(rowData, rowType));
                }
                iterator.close();
                id = scan.checkpoint();

                List<InternalRow> batch = data.remove(id - 1);
                assertThat(result.stream().sorted().collect(Collectors.joining("\n")))
                        .isEqualTo(
                                batch.stream()
                                        .map(
                                                d ->
                                                        DataFormatTestUtil.internalRowToString(
                                                                d, rowType))
                                        .sorted()
                                        .collect(Collectors.joining("\n")));
            } else {
                id = scan.checkpoint();
            }
        }
        Assertions.assertThat(data.values().stream().allMatch(List::isEmpty)).isTrue();
    }

    private void validateSnapshot(Snapshot snapshot, List<TestPojo> data) throws Exception {
        List<Split> splits = table.newSnapshotReader().withSnapshot(snapshot).read().splits();
        List<ConcatRecordReader.ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> table.newRead().createReader(split));
        }
        RecordReader<InternalRow> recordReader = ConcatRecordReader.create(readers);
        RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            InternalRow rowData = iterator.next();
            result.add(DataFormatTestUtil.toStringNoRowKind(rowData, rowType));
        }
        iterator.close();

        assertThat(result).containsExactlyInAnyOrderElementsOf(TestPojo.formatData(data));
    }

    @Test
    public void testCleanOrphanFilesWithChangelogDecoupled() throws Exception {
        // recreate the table with another option
        this.write.close();
        this.commit.close();
        int commitTimes = 30;
        Options options = new Options();
        options.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.INPUT);
        options.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 15);
        options.set(CoreOptions.CHANGELOG_NUM_RETAINED_MAX, 20);
        FileStoreTable table = createFileStoreTable(rowType, options);
        String commitUser = UUID.randomUUID().toString();
        this.table = table;
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);

        List<List<TestPojo>> committedData = new ArrayList<>();
        Map<Long, List<TestPojo>> snapshotData = new HashMap<>();
        Map<Long, List<InternalRow>> changelogData = new HashMap<>();

        SnapshotManager snapshotManager = table.snapshotManager();
        writeData(snapshotManager, committedData, snapshotData, changelogData, commitTimes);

        // generate non used files
        int shouldBeDeleted = generateUnUsedFile();
        assertThat(manuallyAddedFiles.size()).isEqualTo(shouldBeDeleted);

        // first check, nothing will be deleted because the default olderThan interval is 1 day
        OrphanFilesClean orphanFilesClean = new OrphanFilesClean(table);
        assertThat(orphanFilesClean.clean().size()).isEqualTo(0);

        // second check
        orphanFilesClean = new OrphanFilesClean(table);
        setOlderThan(orphanFilesClean);
        orphanFilesClean.clean();
        List<Path> cleanFiles = orphanFilesClean.getDeleteFiles();
        validate(cleanFiles, snapshotData, changelogData);
    }

    /** Manually make a FileNotFoundException to simulate snapshot expire while clean. */
    @Test
    public void testAbnormallyRemoving() throws Exception {
        // generate randomly number of snapshots
        int num = RANDOM.nextInt(5) + 1;
        for (int i = 0; i < num; i++) {
            commit(generateData());
        }

        // randomly delete a manifest file of snapshot 1
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot1 = snapshotManager.snapshot(1);

        List<Path> manifests = new ArrayList<>();
        ManifestList manifestList = table.store().manifestListFactory().create();
        FileStorePathFactory pathFactory = table.store().pathFactory();
        snapshot1
                .allManifests(manifestList)
                .forEach(m -> manifests.add(pathFactory.toManifestFilePath(m.fileName())));

        Path manifest = manifests.get(RANDOM.nextInt(manifests.size()));
        fileIO.deleteQuietly(manifest);

        OrphanFilesClean orphanFilesClean = new OrphanFilesClean(table);
        setOlderThan(orphanFilesClean);
        assertThat(orphanFilesClean.clean().size()).isGreaterThan(0);
    }

    private void writeData(
            SnapshotManager snapshotManager,
            List<List<TestPojo>> committedData,
            Map<Long, List<TestPojo>> snapshotData,
            Map<Long, List<InternalRow>> changelogData,
            int commitTimes)
            throws Exception {
        // first snapshot
        List<TestPojo> data = generateData();
        commit(data);
        committedData.add(data);
        recordSnapshotData(data, snapshotData, snapshotManager);
        recordChangelogData(new ArrayList<>(), data, changelogData, snapshotManager);

        // randomly generate data
        for (int i = 1; i <= commitTimes; i++) {
            List<TestPojo> previous =
                    new ArrayList<>(snapshotData.get(snapshotManager.latestSnapshotId()));
            // randomly update
            if (RANDOM.nextBoolean()) {
                List<TestPojo> toBeUpdated = randomlyPick(previous);
                List<TestPojo> updateAfter = commitUpdate(toBeUpdated);
                committedData.add(updateAfter);

                previous.removeAll(toBeUpdated);
                previous.addAll(updateAfter);
                recordSnapshotData(previous, snapshotData, snapshotManager);
                recordChangelogData(toBeUpdated, updateAfter, changelogData, snapshotManager);
            } else {
                List<TestPojo> current = generateData();
                commit(current);
                committedData.add(current);

                recordChangelogData(new ArrayList<>(), current, changelogData, snapshotManager);
                current.addAll(previous);
                recordSnapshotData(current, snapshotData, snapshotManager);
            }
        }
    }

    private int generateUnUsedFile() throws Exception {
        int shouldBeDeleted = 0;
        int fileNum = RANDOM.nextInt(10);
        fileNum = fileNum == 0 ? 1 : fileNum;

        // snapshot
        addNonUsedFiles(
                new Path(tablePath, "snapshot"), fileNum, Collections.singletonList("UNKNOWN"));

        shouldBeDeleted += fileNum;

        // changelog
        addNonUsedFiles(
                new Path(tablePath, "changelog"), fileNum, Collections.singletonList("UNKNOWN"));

        shouldBeDeleted += fileNum;

        // data files
        shouldBeDeleted += randomlyAddNonUsedDataFiles();

        // manifests
        addNonUsedFiles(
                manifestDir,
                fileNum,
                Arrays.asList("manifest-list-", "manifest-", "index-manifest-", "UNKNOWN-"));
        shouldBeDeleted += fileNum;
        return shouldBeDeleted;
    }

    private void setOlderThan(OrphanFilesClean orphanFilesClean) {
        String timestamp =
                DateTimeUtils.formatLocalDateTime(
                        DateTimeUtils.toLocalDateTime(
                                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2)),
                        3);
        orphanFilesClean.olderThan(timestamp);
    }

    private List<TestPojo> generateData() {
        int num = RANDOM.nextInt(6) + 5;
        List<TestPojo> data = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            data.add(TestPojo.next());
        }
        return data;
    }

    private void commit(List<TestPojo> data) throws Exception {
        for (TestPojo d : data) {
            write.write(d.toRow(RowKind.INSERT));
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
    }

    private List<TestPojo> commitUpdate(List<TestPojo> updates) throws Exception {
        List<TestPojo> after = new ArrayList<>();
        for (TestPojo u : updates) {
            write.write(u.toRow(RowKind.UPDATE_BEFORE));
            TestPojo updateAfter = u.copyWithNewValue();
            after.add(updateAfter);
            write.write(updateAfter.toRow(RowKind.UPDATE_AFTER));
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
        return after;
    }

    private void recordSnapshotData(
            List<TestPojo> data,
            Map<Long, List<TestPojo>> snapshotData,
            SnapshotManager snapshotManager) {
        Snapshot latest = snapshotManager.latestSnapshot();
        if (latest.commitKind() == Snapshot.CommitKind.COMPACT) {
            snapshotData.put(latest.id() - 1, data);
        }
        snapshotData.put(latest.id(), data);
    }

    private void recordChangelogData(
            List<TestPojo> updateBefore,
            List<TestPojo> updateAfter,
            Map<Long, List<InternalRow>> changelogData,
            SnapshotManager snapshotManager) {
        Snapshot latest = snapshotManager.latestSnapshot();
        boolean isInsert = updateBefore.isEmpty();
        if (table.coreOptions().changelogProducer() == CoreOptions.ChangelogProducer.INPUT) {
            List<InternalRow> data = new ArrayList<>();
            for (TestPojo testPojo : updateBefore) {
                data.add(testPojo.toRow(RowKind.UPDATE_BEFORE));
            }
            for (TestPojo testPojo : updateAfter) {
                data.add(testPojo.toRow(isInsert ? RowKind.INSERT : RowKind.UPDATE_AFTER));
            }
            if (latest.commitKind() != Snapshot.CommitKind.COMPACT) {
                changelogData.put(latest.id(), data);
            } else {
                changelogData.put(latest.id() - 1, data);
                changelogData.put(latest.id(), new ArrayList<>());
            }
        } else {
            changelogData.put(latest.id(), new ArrayList<>());
        }
    }

    private int randomlyAddNonUsedDataFiles() throws IOException {
        int addedFiles = 0;
        List<Path> part1 = listSubDirs(tablePath, p -> p.getName().contains("="));
        // add non used file at partition part1
        List<Path> corruptedPartitions = randomlyPick(part1);
        for (Path path : corruptedPartitions) {
            addNonUsedFiles(path, 1, Collections.singletonList("UNKNOWN"));
        }
        addedFiles += corruptedPartitions.size();

        List<Path> part2 = new ArrayList<>();
        for (Path path : part1) {
            part2.addAll(listSubDirs(path, p -> p.getName().contains("=")));
        }

        List<Path> buckets = new ArrayList<>();
        for (Path path : part2) {
            buckets.addAll(listSubDirs(path, p -> p.getName().startsWith(BUCKET_PATH_PREFIX)));
        }

        // add files
        List<Path> corruptedBuckets = randomlyPick(buckets);
        for (Path path : corruptedBuckets) {
            addNonUsedFiles(
                    path, 1, Arrays.asList(DATA_FILE_PREFIX, CHANGELOG_FILE_PREFIX, "UNKNOWN-"));
        }
        addedFiles += corruptedBuckets.size();

        return addedFiles;
    }

    private <T> List<T> randomlyPick(List<T> list) {
        int num = RANDOM.nextInt(list.size());
        num = num == 0 ? 1 : num;
        List<T> copy = new ArrayList<>(list);
        List<T> picked = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            int index = RANDOM.nextInt(copy.size());
            picked.add(copy.remove(index));
        }
        return picked;
    }

    private void addNonUsedFiles(Path dir, int fileNum, List<String> fileNamePrefix)
            throws IOException {
        for (int i = 0; i < fileNum; i++) {
            String fileName =
                    fileNamePrefix.get(RANDOM.nextInt(fileNamePrefix.size())) + UUID.randomUUID();
            Path file = new Path(dir, fileName);
            if (RANDOM.nextBoolean()) {
                fileIO.writeFileUtf8(file, "");
            } else {
                fileIO.mkdirs(file);
            }
            manuallyAddedFiles.add(file);
        }
    }

    private List<Path> listSubDirs(Path root, Predicate<Path> filter) throws IOException {
        return Arrays.stream(fileIO.listStatus(root))
                .map(FileStatus::getPath)
                .filter(filter)
                .collect(Collectors.toList());
    }

    private static class TestPojo {

        private static int increaseKey = 0;

        private final int pk;
        // 0-2
        private final int part1;
        // A-C
        private final String part2;
        private final String value;

        public TestPojo(int pk, int part1, String part2, String value) {
            this.pk = pk;
            this.part1 = part1;
            this.part2 = part2;
            this.value = value;
        }

        public InternalRow toRow(RowKind rowKind) {
            return GenericRow.ofKind(
                    rowKind,
                    pk,
                    part1,
                    BinaryString.fromString(part2),
                    BinaryString.fromString(value));
        }

        public String toInsertValueString() {
            return String.format("(%d, %d, '%s', '%s')", pk, part1, part2, value);
        }

        public TestPojo copyWithNewValue() {
            return new TestPojo(pk, part1, part2, randomValue());
        }

        @Override
        public String toString() {
            return "TestPojo{"
                    + "pk="
                    + pk
                    + ", part1="
                    + part1
                    + ", part2='"
                    + part2
                    + '\''
                    + ", value='"
                    + value
                    + '\''
                    + '}';
        }

        public static void reset() {
            increaseKey = 0;
        }

        public static TestPojo next() {
            int pk = increaseKey++;
            int part1 = RANDOM.nextInt(3);
            char c = (char) (RANDOM.nextInt(3) + 'A');
            String part2 = String.valueOf(c);
            String value = randomValue();
            return new TestPojo(pk, part1, part2, value);
        }

        public static List<String> formatData(List<TestPojo> data) {
            return data.stream().map(TestPojo::format).collect(Collectors.toList());
        }

        private String format() {
            return String.format("%d, %d, %s, %s", pk, part1, part2, value);
        }
    }

    private static String randomValue() {
        return StringUtils.getRandomString(RANDOM, 5, 20, 'a', 'z');
    }

    private FileStoreTable createFileStoreTable(RowType rowType, Options conf) throws Exception {
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, RANDOM.nextInt(3) + 1);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("part1", "part2"),
                                Arrays.asList("pk", "part1", "part2"),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }
}
