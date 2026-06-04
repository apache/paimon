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

package org.apache.paimon.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ManifestFileMeta}. */
public class ManifestFileMetaTest extends ManifestFileMetaTestBase {

    private static final RowType PARTITION_TYPE = RowType.of(new IntType());

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @Disabled // TODO wrong test to rely on self-defined file size
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4})
    public void testMergeWithoutFullCompaction(int numLastBits) {
        List<ManifestFileMeta> input = new ArrayList<>();
        List<ManifestFileMeta> expected = new ArrayList<>();
        createData(numLastBits, input, expected);

        // no trigger Full Compaction
        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "9223372036854775807B");
        List<ManifestFileMeta> actual =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertThat(actual).hasSameSizeAs(expected);

        // these two manifest files are merged from the input
        assertSameContent(expected.get(0), actual.get(0), manifestFile);
        assertSameContent(expected.get(1), actual.get(1), manifestFile);

        // these two manifest files should be kept without modification
        assertThat(actual.get(2)).isEqualTo(input.get(5));
        assertThat(actual.get(3)).isEqualTo(input.get(6));

        // check last bits
        for (int i = 4; i < actual.size(); i++) {
            assertSameContent(expected.get(i), actual.get(i), manifestFile);
        }
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        List<ManifestFileMeta> input = new ArrayList<>();
        createData(ThreadLocalRandom.current().nextInt(5), input, null);
        // merge without fullcompaction
        testCleanUp(input, Long.MAX_VALUE);
    }

    private void testCleanUp(List<ManifestFileMeta> input, long fullCompactionThreshold)
            throws IOException {
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 1, 10);
        ManifestFile failingManifestFile =
                createManifestFile(FailingFileIO.getFailingPath(failingName, tempDir.toString()));
        try {
            Options testOptions = new Options();
            testOptions.set("manifest.target-file-size", "500B");
            testOptions.set("manifest.merge-min-count", "3");
            testOptions.set(
                    "manifest.full-compaction-threshold-size", fullCompactionThreshold + "B");
            ManifestFileMerger.merge(
                    input,
                    failingManifestFile,
                    getPartitionType(),
                    CoreOptions.fromMap(testOptions.toMap()));
        } catch (Throwable e) {
            assertThat(e).hasRootCauseExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            // old files should be kept untouched, while new files should be cleaned up
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            assertThat(
                            new TreeSet<>(
                                    Arrays.stream(LocalFileIO.create().listStatus(manifestDir))
                                            .map(s -> s.getPath().getName())
                                            .collect(Collectors.toList())))
                    .isEqualTo(
                            new TreeSet<>(
                                    input.stream()
                                            .map(ManifestFileMeta::fileName)
                                            .collect(Collectors.toList())));
        }
    }

    @RepeatedTest(10)
    public void testCleanUpWithFullCompaction() throws IOException {
        List<ManifestFileMeta> input = new ArrayList<>();
        createData(ThreadLocalRandom.current().nextInt(5), input, null);
        testCleanUp(input, 1L);
    }

    @Test
    public void testMerge() {
        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);
        // delta with delete apply partition 1,2
        addDeltaManifests(input, true);
        // trigger full compaction
        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        // 1st Manifest don't need to Merge
        assertSameContent(input.get(0), merged.get(0), manifestFile);

        // after merge the entry is equivalent
        assertEquivalentEntries(input, merged);
    }

    @Test
    public void testMergeWithoutDelta() {

        // base
        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);

        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        assertEquivalentEntries(input, merged);
        assertThat(merged).hasSameElementsAs(input);

        // Delete and Add are completely neutralized in  delta.
        List<ManifestFileMeta> base = createBaseManifestFileMetas(true);
        List<ManifestFileMeta> input1 = new ArrayList<>(base);
        ManifestFileMeta delta = makeManifest(makeEntry(true, "A", 1), makeEntry(false, "A", 1));
        input1.add(delta);

        Options testOptions1 = new Options();
        testOptions1.set("manifest.target-file-size", "500B");
        testOptions1.set("manifest.merge-min-count", "3");
        testOptions1.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged1 =
                ManifestFileMerger.merge(
                        input1,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions1.toMap()));

        assertThat(base).hasSameElementsAs(merged1);
        assertEquivalentEntries(input1, merged1);
    }

    @Test
    public void testMergeWithoutBase() {
        List<ManifestFileMeta> input = new ArrayList<>();
        addDeltaManifests(input, true);
        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertEquivalentEntries(input, merged);
    }

    @Test
    public void testMergeWithoutDeleteFile() {
        // entries are All ADD.
        List<ManifestFileMeta> input = new ArrayList<>();
        // base
        for (int j = 0; j < 6; j++) {
            List<ManifestEntry> entrys = new ArrayList<>();
            for (int i = 1; i < 16; i++) {
                entrys.add(makeEntry(true, String.format(manifestFileNameTemplate, j, i), j));
            }
            input.add(makeManifest(entrys.toArray(new ManifestEntry[0])));
        }
        // delta
        input.add(makeManifest(makeEntry(true, "A")));
        input.add(makeManifest(makeEntry(true, "B")));
        input.add(makeManifest(makeEntry(true, "C")));
        input.add(makeManifest(makeEntry(true, "D")));
        input.add(makeManifest(makeEntry(true, "E")));
        input.add(makeManifest(makeEntry(true, "F")));
        input.add(makeManifest(makeEntry(true, "G")));

        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", "500B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertEquivalentEntries(input, merged);
    }

    @Test
    public void testTriggerFullCompaction() throws Exception {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            entries.add(makeEntry(true, String.valueOf(i)));
        }

        List<ManifestFileMeta> input = new ArrayList<>();

        // base manifest
        ManifestFileMeta manifest1 = makeManifest(entries.toArray(new ManifestEntry[0]));

        // delta manifest
        ManifestFileMeta manifest2 =
                makeManifest(makeEntry(true, "A"), makeEntry(true, "B"), makeEntry(true, "C"));
        ManifestFileMeta manifest3 = makeManifest(makeEntry(true, "D"));
        ManifestFileMeta manifest4 =
                makeManifest(makeEntry(false, "A"), makeEntry(false, "B"), makeEntry(true, "F"));
        ManifestFileMeta manifest5 = makeManifest(makeEntry(false, "14"), makeEntry(false, "15"));
        ManifestFileMeta manifest6 =
                makeManifest(
                        makeEntry(false, "C"),
                        makeEntry(false, "D"),
                        makeEntry(false, "F"),
                        makeEntry(true, "G"));
        ManifestFileMeta manifest7 =
                makeManifest(makeEntry(true, "H"), makeEntry(true, "I"), makeEntry(false, "H"));

        // no trigger for delta size
        // case1: the total file size which must change do not reach the sizeTrigger
        input.addAll(
                Arrays.asList(manifest1, manifest2, manifest3, manifest4, manifest5, manifest6));
        List<ManifestFileMeta> newMetas1 = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted1 =
                ManifestFileMerger.tryFullCompaction(
                        input,
                        newMetas1,
                        manifestFile,
                        500,
                        Long.MAX_VALUE,
                        getPartitionType(),
                        null);
        assertThat(fullCompacted1).isEmpty();
        assertThat(newMetas1).isEmpty();

        // case2: only one manifest file in input, it won't be rewritten
        input.clear();
        input.add(manifest7);
        List<ManifestFileMeta> newMetas2 = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted2 =
                ManifestFileMerger.tryFullCompaction(
                        input, newMetas1, manifestFile, 500, 100, getPartitionType(), null);
        assertThat(fullCompacted2).isEmpty();
        assertThat(newMetas2).isEmpty();

        // case3: all the manifest files have sizes exceeding suggestedMetaSize, and have no delete
        // files
        input.clear();
        input.addAll(Arrays.asList(manifest1, manifest2, manifest3));
        List<ManifestFileMeta> newMetas3 = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted3 =
                ManifestFileMerger.tryFullCompaction(
                        input, newMetas3, manifestFile, 500, 100, getPartitionType(), null);
        assertThat(fullCompacted3).isEmpty();
        assertThat(newMetas3).isEmpty();

        // case4: the sizes of all the manifest files are less than suggestedMetaSize, and have no
        // delete files
        input.clear();
        input.addAll(Arrays.asList(manifest1, manifest2, manifest3));
        List<ManifestFileMeta> newMetas4 = new ArrayList<>();
        List<ManifestFileMeta> fullCompacted4 =
                ManifestFileMerger.tryFullCompaction(
                                input, newMetas4, manifestFile, 5000, 100, getPartitionType(), null)
                        .get();
        assertThat(fullCompacted4.size()).isEqualTo(1);
        assertThat(newMetas4.size()).isEqualTo(1);

        // case5:the sizes of some manifest files are greater than suggestedMetaSize, while the
        // sizes of other manifest files is less than. All manifest files have no delete files
        input.clear();
        input.addAll(Arrays.asList(manifest1, manifest2, manifest3, manifest4));
        List<ManifestFileMeta> newMetas5 = new ArrayList<>();
        List<ManifestFileMeta> fullCompacted5 =
                ManifestFileMerger.tryFullCompaction(
                                input, newMetas5, manifestFile, 1800, 100, getPartitionType(), null)
                        .get();
        assertThat(fullCompacted5.size()).isEqualTo(3);
        assertThat(newMetas5.size()).isEqualTo(1);

        // trigger full compaction
        // case6: sizes of all files are greater than suggestedMinMetaCount, m files has no delete
        // entries while n files have deleted entries
        input.clear();
        input.addAll(
                Arrays.asList(
                        manifest1, manifest2, manifest3, manifest4, manifest5, manifest6,
                        manifest7));
        List<ManifestFileMeta> newMetas6 = new ArrayList<>();
        List<ManifestFileMeta> fullCompacted6 =
                ManifestFileMerger.tryFullCompaction(
                                input, newMetas6, manifestFile, 500, 100, getPartitionType(), null)
                        .get();

        List<String> entryFileNameExptected = new ArrayList<>(Arrays.asList("ADD-G", "ADD-I"));
        for (int i = 0; i < 14; i++) {
            entryFileNameExptected.add("ADD-" + i);
        }
        containSameEntryFile(fullCompacted6, entryFileNameExptected);

        // case7: sizeTrigger = 0
        List<ManifestFileMeta> newMetas7 = new ArrayList<>();
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    ManifestFileMerger.tryFullCompaction(
                            input, newMetas7, manifestFile, 500, 0, getPartitionType(), null);
                });

        // case8: manifest file is deleted when reading
        List<ManifestFileMeta> newMetas8 = new ArrayList<>();
        manifestFile.delete(manifest7.fileName());
        assertThrows(
                Exception.class,
                () -> {
                    ManifestFileMerger.tryFullCompaction(
                            input, newMetas8, manifestFile, 500, 100, getPartitionType(), null);
                });
        assertThat(newMetas8).isEmpty();
    }

    @Test
    public void testMultiPartitionsFullCompaction() throws Exception {

        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);

        // delta with delete apply partition 1,2
        addDeltaManifests(input, true);

        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> mergedManifest =
                ManifestFileMerger.tryFullCompaction(
                                input, newMetas, manifestFile, 500, 100, getPartitionType(), null)
                        .get();

        List<String> expected = Lists.newArrayList("ADD-C2", "ADD-D2", "ADD-G");
        HashMap<Integer, Integer> partitionAndFileLimit =
                new HashMap<Integer, Integer>() {
                    {
                        this.put(0, 16);
                        this.put(1, 14);
                        this.put(2, 14);
                    }
                };
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < partitionAndFileLimit.get(i); j++) {
                expected.add(String.format("ADD-" + manifestFileNameTemplate, i, j));
            }
        }

        assertThat(mergedManifest.size()).isEqualTo(2);
        assertThat(newMetas.size()).isEqualTo(1);

        containSameEntryFile(mergedManifest, expected);
    }

    @Test
    public void testIdentifierAfterFullCompaction() throws Exception {
        List<ManifestEntry> entries = new ArrayList<>();
        List<ManifestEntry> expectedEntries = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            entries.add(
                    makeEntry(
                            true,
                            String.valueOf(i),
                            0,
                            1,
                            Lists.newArrayList("extra-" + i),
                            new byte[] {1, 2, 3}));
        }
        expectedEntries.addAll(entries.subList(0, 15));

        ManifestFileMeta manifest1 = makeManifest(entries.toArray(new ManifestEntry[0]));

        entries.clear();
        entries.add(
                makeEntry(true, "A", 0, 2, Lists.newArrayList("extra-A"), new byte[] {1, 2, 3}));
        entries.add(
                makeEntry(true, "B", 0, 2, Lists.newArrayList("extra-B"), new byte[] {1, 2, 3}));
        entries.add(
                makeEntry(true, "C", 0, 2, Lists.newArrayList("extra-C"), new byte[] {1, 2, 3}));
        expectedEntries.add(entries.get(2));
        ManifestFileMeta manifest2 = makeManifest(entries.toArray(new ManifestEntry[0]));

        entries.clear();
        entries.add(
                makeEntry(false, "A", 0, 2, Lists.newArrayList("extra-A"), new byte[] {1, 2, 3}));
        entries.add(
                makeEntry(false, "B", 0, 2, Lists.newArrayList("extra-B"), new byte[] {1, 2, 3}));
        entries.add(
                makeEntry(true, "F", 0, 3, Lists.newArrayList("extra-F"), new byte[] {1, 2, 3}));
        expectedEntries.add(entries.get(2));
        ManifestFileMeta manifest3 = makeManifest(entries.toArray(new ManifestEntry[0]));

        entries.clear();
        entries.add(
                makeEntry(false, "14", 0, 0, Lists.newArrayList("extra-14"), new byte[] {1, 2, 3}));
        entries.add(
                makeEntry(false, "15", 0, 1, Lists.newArrayList("extra-15"), new byte[] {1, 2, 3}));
        ManifestFileMeta manifest4 = makeManifest(entries.toArray(new ManifestEntry[0]));

        List<ManifestFileMeta> input =
                new ArrayList<>(Arrays.asList(manifest1, manifest2, manifest3, manifest4));
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> fullCompacted =
                ManifestFileMerger.tryFullCompaction(
                                input, newMetas, manifestFile, 500, 100, getPartitionType(), null)
                        .get();
        assertThat(fullCompacted.size()).isEqualTo(1);
        assertThat(newMetas.size()).isEqualTo(1);
        List<FileEntry.Identifier> entryIdentifierExpected =
                expectedEntries.stream()
                        .map(ManifestEntry::identifier)
                        .collect(Collectors.toList());
        containSameIdentifyEntryFile(fullCompacted, entryIdentifierExpected);
    }

    @Test
    public void testMergeFullCompactionWithoutDeleteFile() {
        // entries are All ADD.
        List<ManifestFileMeta> input = new ArrayList<>();
        // base
        for (int j = 0; j < 6; j++) {
            List<ManifestEntry> entrys = new ArrayList<>();
            for (int i = 1; i < 50; i++) {
                entrys.add(makeEntry(true, String.format(manifestFileNameTemplate, j, i), j));
            }
            input.add(makeManifest(entrys.toArray(new ManifestEntry[0])));
        }
        // The base file all meet the manifest file size.
        long threshold = input.stream().mapToLong(ManifestFileMeta::fileSize).min().getAsLong();
        Set<String> baseFiles =
                input.stream().map(ManifestFileMeta::fileName).collect(Collectors.toSet());

        // assert base manifest are not accessed
        for (String baseFile : baseFiles) {
            manifestFile.delete(baseFile);
        }

        // delta
        input.add(makeManifest(makeEntry(true, "A")));
        input.add(makeManifest(makeEntry(true, "B")));
        input.add(makeManifest(makeEntry(true, "C")));
        input.add(makeManifest(makeEntry(true, "D")));
        input.add(makeManifest(makeEntry(true, "E")));
        input.add(makeManifest(makeEntry(true, "F")));
        input.add(makeManifest(makeEntry(true, "G")));

        Options testOptions = new Options();
        testOptions.set("manifest.target-file-size", threshold + "B");
        testOptions.set("manifest.merge-min-count", "3");
        testOptions.set("manifest.full-compaction-threshold-size", "200B");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));
        assertEquivalentEntries(
                input.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()),
                merged.stream()
                        .filter(f -> !baseFiles.contains(f.fileName()))
                        .collect(Collectors.toList()));
    }

    @Test
    public void testFullCompactionReadManifestsInParallel() throws Exception {
        BlockingReadFileIO fileIO = new BlockingReadFileIO();
        manifestFile = createManifestFile(tempDir.toString(), fileIO);

        List<ManifestFileMeta> input = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            input.add(makeManifest(makeEntry(true, "parallel-" + i)));
        }

        List<ManifestFileMeta> newMetas = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted;
        fileIO.blockManifestReads();
        try {
            fullCompacted =
                    ManifestFileMerger.tryFullCompaction(
                            input,
                            newMetas,
                            manifestFile,
                            Long.MAX_VALUE,
                            1,
                            getPartitionType(),
                            2);
        } finally {
            fileIO.stopBlockingManifestReads();
        }

        assertThat(fileIO.maxConcurrentManifestReads()).isGreaterThanOrEqualTo(2);
        assertThat(fullCompacted).isPresent();
        assertEquivalentEntries(input, fullCompacted.get());
    }

    @RepeatedTest(10)
    public void testRandomFullCompaction() throws Exception {
        List<ManifestFileMeta> input = new ArrayList<>();
        Set<FileEntry.Identifier> manifestEntrySet = new HashSet<>();
        Set<FileEntry.Identifier> deleteManifestEntrySet = new HashSet<>();
        int inputSize = ThreadLocalRandom.current().nextInt(100) + 1;
        int totalEntryNums = 0;
        for (int i = 0; i < inputSize; i++) {
            int entryNums = ThreadLocalRandom.current().nextInt(100) + 1;
            input.add(
                    generateRandomData(
                            entryNums, totalEntryNums, manifestEntrySet, deleteManifestEntrySet));
            totalEntryNums += entryNums;
        }
        int suggerstSize = ThreadLocalRandom.current().nextInt(3000) + 1;
        int sizeTrigger = ThreadLocalRandom.current().nextInt(40000) + 1;
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted =
                ManifestFileMerger.tryFullCompaction(
                        input,
                        newMetas,
                        manifestFile,
                        suggerstSize,
                        sizeTrigger,
                        getPartitionType(),
                        null);

        // *****verify result*****
        List<ManifestFileMeta> mustMergedFiles =
                input.stream()
                        .filter(
                                manifest ->
                                        manifest.fileSize() < suggerstSize
                                                || manifest.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        long mustMergeSize =
                mustMergedFiles.stream().map(ManifestFileMeta::fileSize).reduce(0L, Long::sum);
        // manifest files which might be merged
        List<ManifestFileMeta> toBeMerged = new LinkedList<>(input);
        if (getPartitionType().getFieldCount() > 0) {
            Set<BinaryRow> deletePartitions =
                    deleteManifestEntrySet.stream()
                            .map(entry -> entry.partition)
                            .collect(Collectors.toSet());
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(getPartitionType(), deletePartitions);
            if (predicate != null) {
                Iterator<ManifestFileMeta> iterator = toBeMerged.iterator();
                while (iterator.hasNext()) {
                    ManifestFileMeta file = iterator.next();
                    if (file.fileSize() < suggerstSize || file.numDeletedFiles() > 0) {
                        continue;
                    }
                    if (!predicate.test(
                            file.numAddedFiles() + file.numDeletedFiles(),
                            file.partitionStats().minValues(),
                            file.partitionStats().maxValues(),
                            file.partitionStats().nullCounts())) {
                        iterator.remove();
                    }
                }
            }
        }
        // manifest files which were not written after full compaction
        List<ManifestFileMeta> notMergedFiles =
                input.stream()
                        .filter(
                                manifest ->
                                        manifest.fileSize() >= suggerstSize
                                                && manifest.numDeletedFiles() == 0)
                        .filter(
                                manifest ->
                                        manifestFile.read(manifest.fileName(), manifest.fileSize())
                                                .stream()
                                                .map(ManifestEntry::identifier)
                                                .noneMatch(deleteManifestEntrySet::contains))
                        .collect(Collectors.toList());

        if (mustMergeSize < sizeTrigger) {
            assertThat(fullCompacted).isEmpty();
            assertThat(newMetas).isEmpty();
        } else if (toBeMerged.size() <= 1) {
            assertThat(fullCompacted).isEmpty();
            assertThat(newMetas).isEmpty();
        } else {
            assertThat(fullCompacted.get().size()).isEqualTo(notMergedFiles.size() + 1);
            assertThat(newMetas).size().isEqualTo(1);
        }
    }

    private ManifestFileMeta generateRandomData(
            int entryNums,
            int totalEntryNums,
            Set<FileEntry.Identifier> manifestEntrySet,
            Set<FileEntry.Identifier> deleteManifestEntrySet) {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryNums; i++) {
            // 70% add, 30% delete
            boolean isAdd = ThreadLocalRandom.current().nextInt(10) < 7;
            if (manifestEntrySet.isEmpty() || isAdd) {
                String fileName = String.format("file-%d", totalEntryNums + i);
                Integer partition = ThreadLocalRandom.current().nextInt(10);
                int level = ThreadLocalRandom.current().nextInt(6);
                List<String> extraFiles = Lists.newArrayList(String.format("index-%s", fileName));
                byte[] embeddedIndex = new byte[] {1, 2, 3};
                entries.add(makeEntry(true, fileName, partition, level, extraFiles, embeddedIndex));
            } else {
                FileEntry.Identifier identifier =
                        manifestEntrySet.stream()
                                .skip(ThreadLocalRandom.current().nextInt(manifestEntrySet.size()))
                                .findFirst()
                                .get();
                entries.add(
                        makeEntry(
                                false,
                                identifier.fileName,
                                identifier.partition.getInt(0),
                                identifier.level,
                                identifier.extraFiles,
                                new byte[] {1, 2, 3}));
                manifestEntrySet.remove(identifier);
                deleteManifestEntrySet.add(identifier);
            }
        }
        manifestEntrySet.addAll(
                entries.stream().map(ManifestEntry::identifier).collect(Collectors.toSet()));
        return makeManifest(entries.toArray(new ManifestEntry[0]));
    }

    private void createData(
            int numLastBits, List<ManifestFileMeta> input, List<ManifestFileMeta> expected) {
        // suggested size 500 and suggested count 3
        // file sizes:
        // 200, 300, -- multiple files exactly the suggested size
        // 100, 200, 300, -- multiple files exceeding the suggested size
        // 500, -- single file exactly the suggested size
        // 600, -- single file exceeding the suggested size
        // 100 * numLastBits -- the last bit

        input.add(makeManifest(makeEntry(true, "A"), makeEntry(true, "B")));
        input.add(makeManifest(makeEntry(true, "C"), makeEntry(false, "B"), makeEntry(true, "D")));

        input.add(makeManifest(makeEntry(false, "A")));
        input.add(makeManifest(makeEntry(true, "E"), makeEntry(true, "F")));
        input.add(makeManifest(makeEntry(true, "G"), makeEntry(false, "E"), makeEntry(false, "G")));

        input.add(
                makeManifest(
                        makeEntry(false, "C"),
                        makeEntry(false, "F"),
                        makeEntry(true, "H"),
                        makeEntry(true, "I"),
                        makeEntry(false, "H")));

        input.add(
                makeManifest(
                        makeEntry(false, "I"),
                        makeEntry(true, "J"),
                        makeEntry(true, "K"),
                        makeEntry(false, "J"),
                        makeEntry(false, "K"),
                        makeEntry(true, "L")));

        for (int i = 0; i < numLastBits; i++) {
            input.add(makeManifest(makeEntry(true, String.valueOf(i))));
        }

        if (expected == null) {
            return;
        }

        expected.add(
                makeManifest(makeEntry(true, "A"), makeEntry(true, "C"), makeEntry(true, "D")));
        expected.add(makeManifest(makeEntry(false, "A"), makeEntry(true, "F")));
        expected.add(input.get(5));
        expected.add(input.get(6));

        if (numLastBits < 3) {
            for (int i = 0; i < numLastBits; i++) {
                expected.add(input.get(7 + i));
            }
        } else {
            expected.add(
                    makeManifest(
                            IntStream.range(0, numLastBits)
                                    .mapToObj(i -> makeEntry(true, String.valueOf(i)))
                                    .toArray(ManifestEntry[]::new)));
        }
    }

    public RowType getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    ManifestFile getManifestFile() {
        return manifestFile;
    }

    private static class BlockingReadFileIO extends LocalFileIO {

        private final AtomicBoolean blockManifestReads = new AtomicBoolean(false);
        private final AtomicInteger activeManifestReads = new AtomicInteger(0);
        private final AtomicInteger maxConcurrentManifestReads = new AtomicInteger(0);
        private final CountDownLatch readersReady = new CountDownLatch(2);
        private final CountDownLatch releaseReaders = new CountDownLatch(1);

        private void blockManifestReads() {
            blockManifestReads.set(true);
        }

        private void stopBlockingManifestReads() {
            blockManifestReads.set(false);
            releaseReaders.countDown();
        }

        private int maxConcurrentManifestReads() {
            return maxConcurrentManifestReads.get();
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream inputStream = super.newInputStream(path);
            if (!blockManifestReads.get() || !path.toString().contains("/manifest/")) {
                return inputStream;
            }
            return new BlockingSeekableInputStream(inputStream);
        }

        private class BlockingSeekableInputStream extends SeekableInputStreamWrapper {

            private boolean entered;
            private boolean closed;

            private BlockingSeekableInputStream(SeekableInputStream inputStream) {
                super(inputStream);
            }

            @Override
            public int read() throws IOException {
                beforeFirstRead();
                return super.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                beforeFirstRead();
                return super.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    if (entered && !closed) {
                        activeManifestReads.decrementAndGet();
                    }
                    closed = true;
                }
            }

            private void beforeFirstRead() throws IOException {
                if (entered) {
                    return;
                }

                entered = true;
                int activeReads = activeManifestReads.incrementAndGet();
                maxConcurrentManifestReads.accumulateAndGet(activeReads, Math::max);
                readersReady.countDown();
                if (readersReady.getCount() == 0) {
                    releaseReaders.countDown();
                }

                try {
                    if (!releaseReaders.await(3, TimeUnit.SECONDS)) {
                        throw new IOException("Manifest reads were not parallelized.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
        }
    }

    // ==================== Manifest Sort Tests ====================

    /**
     * Test manifest sort with overlapping partition ranges. Each manifest contains entries spanning
     * multiple partitions, creating overlapping intervals that require sort rewrite to resolve.
     * After sort rewrite, all surviving ADD entries should be sorted by partition field.
     */
    @Test
    public void testManifestSortWithOverlappingPartitions() {
        List<ManifestFileMeta> input = new ArrayList<>();

        // manifest-A: partitions [5, 13]
        List<ManifestEntry> entriesA = new ArrayList<>();
        for (int p = 5; p <= 13; p++) {
            entriesA.add(makeEntry(true, String.format("A-p%d", p), p));
        }
        input.add(makeManifest(entriesA.toArray(new ManifestEntry[0])));

        // manifest-B: partitions [0, 9]
        List<ManifestEntry> entriesB = new ArrayList<>();
        for (int p = 0; p <= 9; p++) {
            entriesB.add(makeEntry(true, String.format("B-p%d", p), p));
        }
        input.add(makeManifest(entriesB.toArray(new ManifestEntry[0])));

        // manifest-C: partitions [3, 7] -- overlaps with A and B
        List<ManifestEntry> entriesC = new ArrayList<>();
        for (int p = 3; p <= 7; p++) {
            entriesC.add(makeEntry(true, String.format("C-p%d", p), p));
        }
        input.add(makeManifest(entriesC.toArray(new ManifestEntry[0])));

        // manifest-D: partitions [8, 12] -- overlaps with A
        List<ManifestEntry> entriesD = new ArrayList<>();
        for (int p = 8; p <= 12; p++) {
            entriesD.add(makeEntry(true, String.format("D-p%d", p), p));
        }
        input.add(makeManifest(entriesD.toArray(new ManifestEntry[0])));

        // manifest-E: partitions [1, 6] -- overlaps with B and C
        List<ManifestEntry> entriesE = new ArrayList<>();
        for (int p = 1; p <= 6; p++) {
            entriesE.add(makeEntry(true, String.format("E-p%d", p), p));
        }
        input.add(makeManifest(entriesE.toArray(new ManifestEntry[0])));

        // manifest-F: partitions [4, 14] -- overlaps with D
        List<ManifestEntry> entriesF = new ArrayList<>();
        for (int p = 4; p <= 14; p++) {
            entriesF.add(makeEntry(true, String.format("F-p%d", p), p));
        }
        input.add(makeManifest(entriesF.toArray(new ManifestEntry[0])));

        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        // Verify entries are equivalent (no data loss)
        assertEquivalentEntries(input, merged);

        // Verify all entries within each output manifest are sorted by partition
        for (ManifestFileMeta meta : merged) {
            List<ManifestEntry> entries = manifestFile.read(meta.fileName(), meta.fileSize());
            for (int i = 1; i < entries.size(); i++) {
                int prevPartition = entries.get(i - 1).partition().getInt(0);
                int currPartition = entries.get(i).partition().getInt(0);
                assertThat(currPartition)
                        .as("Entries within a manifest should be sorted by partition")
                        .isGreaterThanOrEqualTo(prevPartition);
            }
        }
    }

    /**
     * Test that sort rewrite correctly eliminates DELETE entries and their corresponding ADD
     * entries. The key condition is that totalDeltaFileSize must reach manifestFullCompactionSize
     * to trigger the full compaction path inside trySortRewrite, which reads deleteEntries and
     * passes them to sortAndRewriteSection for elimination.
     *
     * <p>Design:
     *
     * <pre>
     *   - Base manifests with overlapping partitions (all ADD, large enough to be "mustChange"
     *     since fileSize < suggestedMetaSize):
     *     manifest-A: partitions [0, 4] with entries A-p0..A-p4
     *     manifest-B: partitions [2, 6] with entries B-p2..B-p6 (overlaps A)
     *     manifest-C: partitions [5, 9] with entries C-p5..C-p9 (overlaps B)
     *   - Delta manifests with DELETE entries (cancel some ADD entries):
     *     manifest-D: DELETE A-p2, DELETE B-p4, ADD new-p2, ADD new-p4
     *     manifest-E: DELETE C-p7, ADD new-p7
     *   - After sort rewrite: A-p2, B-p4, C-p7 should be eliminated,
     *     replaced by new-p2, new-p4, new-p7. Output should only contain ADD entries,
     *     sorted by partition.
     * </pre>
     */
    @Test
    public void testManifestSortEliminatesDeleteEntries() {
        List<ManifestFileMeta> input = new ArrayList<>();

        // manifest-A: partitions [0, 4]
        List<ManifestEntry> entriesA = new ArrayList<>();
        for (int p = 0; p <= 4; p++) {
            entriesA.add(makeEntry(true, String.format("A-p%d", p), p));
        }
        input.add(makeManifest(entriesA.toArray(new ManifestEntry[0])));

        // manifest-B: partitions [2, 6] -- overlaps A
        List<ManifestEntry> entriesB = new ArrayList<>();
        for (int p = 2; p <= 6; p++) {
            entriesB.add(makeEntry(true, String.format("B-p%d", p), p));
        }
        input.add(makeManifest(entriesB.toArray(new ManifestEntry[0])));

        // manifest-C: partitions [5, 9] -- overlaps B
        List<ManifestEntry> entriesC = new ArrayList<>();
        for (int p = 5; p <= 9; p++) {
            entriesC.add(makeEntry(true, String.format("C-p%d", p), p));
        }
        input.add(makeManifest(entriesC.toArray(new ManifestEntry[0])));

        // manifest-D: DELETE A-p2, DELETE B-p4, ADD new-p2, ADD new-p4
        input.add(
                makeManifest(
                        makeEntry(false, "A-p2", 2),
                        makeEntry(false, "B-p4", 4),
                        makeEntry(true, "new-p2", 2),
                        makeEntry(true, "new-p4", 4)));

        // manifest-E: DELETE C-p7, ADD new-p7
        input.add(makeManifest(makeEntry(false, "C-p7", 7), makeEntry(true, "new-p7", 7)));

        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");
        testOptions.set("manifest.full-compaction-threshold-size", "10B");

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        // Collect all output entries
        List<ManifestEntry> allOutputEntries = new ArrayList<>();
        for (ManifestFileMeta meta : merged) {
            allOutputEntries.addAll(manifestFile.read(meta.fileName(), meta.fileSize()));
        }

        // Verify: no DELETE entries in output (all DELETE pairs eliminated)
        long deleteCount =
                allOutputEntries.stream().filter(e -> e.kind() == FileKind.DELETE).count();
        assertThat(deleteCount).as("Sort rewrite should eliminate all DELETE entries").isEqualTo(0);

        // Verify: the deleted ADD entries (A-p2, B-p4, C-p7) are NOT in output
        Set<String> outputFileNames =
                allOutputEntries.stream().map(e -> e.file().fileName()).collect(Collectors.toSet());
        assertThat(outputFileNames).doesNotContain("A-p2", "B-p4", "C-p7");

        // Verify: the replacement entries (new-p2, new-p4, new-p7) ARE in output
        assertThat(outputFileNames).contains("new-p2", "new-p4", "new-p7");

        // Verify: all surviving entries match what FileEntry.mergeEntries would produce
        assertEquivalentEntries(input, merged);

        // Verify entries within each output manifest are sorted by partition
        for (ManifestFileMeta meta : merged) {
            List<ManifestEntry> entries = manifestFile.read(meta.fileName(), meta.fileSize());
            for (int i = 1; i < entries.size(); i++) {
                int prevPartition = entries.get(i - 1).partition().getInt(0);
                int currPartition = entries.get(i).partition().getInt(0);
                assertThat(currPartition)
                        .as("Entries within manifest should be sorted by partition")
                        .isGreaterThanOrEqualTo(prevPartition);
            }
        }
    }

    /**
     * Test manifest sort with a multi-field partition type.
     *
     * <p>Setup: partition=(region INT, dt INT, hour INT), sort by dt (field index=1). 9 manifest
     * files form 6 overlapping sorted runs by dt range:
     *
     * <pre>
     *   Run1: 3 files, dt=[0,15],[3,5],[6,8]
     *   Run2: 2 files, dt=[1,8],[5,7]
     *   Run3: 1 file,  dt=[0,9]
     *   Run4: 1 file,  dt=[5,14]
     *   Run5: 1 file,  dt=[8,15]
     *   Run6: 1 file,  dt=[4,12]
     * </pre>
     *
     * <p>Verifies: 1) no data loss after sort-rewrite, 2) entries within each output manifest are
     * sorted by dt.
     */
    @Test
    public void testManifestSortWithMultiplePartitions() {
        // Use a 3-field partition type: (region INT, dt INT, hour INT)
        RowType multiPartitionType = RowType.of(new IntType(), new IntType(), new IntType());

        // Create a dedicated ManifestFile for the 3-field partition type
        Path path = new Path(tempDir.toString());
        FileIO fileIO = FileIOFinder.find(path);
        ManifestFile multiPartManifestFile =
                new ManifestFile.Factory(
                                fileIO,
                                new SchemaManager(fileIO, path),
                                multiPartitionType,
                                avro,
                                "zstd",
                                new FileStorePathFactory(
                                        path,
                                        multiPartitionType,
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
                                Long.MAX_VALUE,
                                null)
                        .create();

        List<ManifestFileMeta> input = new ArrayList<>();

        // Run1
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r1a-p0", 10, 0, 1),
                                        makeMultiPartEntry(true, "r1a-p1", 20, 1, 2),
                                        makeMultiPartEntry(true, "r1a-p2", 30, 15, 3)))
                        .get(0));
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r1b-p3", 10, 3, 4),
                                        makeMultiPartEntry(true, "r1b-p4", 20, 4, 5),
                                        makeMultiPartEntry(true, "r1b-p5", 30, 5, 6)))
                        .get(0));
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r1c-p6", 10, 6, 7),
                                        makeMultiPartEntry(true, "r1c-p7", 20, 7, 8),
                                        makeMultiPartEntry(true, "r1c-p8", 30, 8, 9)))
                        .get(0));

        // Run2
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r2a-p1", 5, 1, 10),
                                        makeMultiPartEntry(true, "r2a-p2", 15, 2, 11),
                                        makeMultiPartEntry(true, "r2a-p3", 25, 3, 12),
                                        makeMultiPartEntry(true, "r2a-p4", 35, 8, 13)))
                        .get(0));
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r2b-p5", 5, 5, 14),
                                        makeMultiPartEntry(true, "r2b-p6", 15, 6, 15),
                                        makeMultiPartEntry(true, "r2b-p7", 25, 7, 16)))
                        .get(0));

        // Run3
        List<ManifestEntry> run3Entries = new ArrayList<>();
        for (int p = 0; p <= 9; p++) {
            run3Entries.add(makeMultiPartEntry(true, String.format("r3-p%d", p), 99, p, p + 20));
        }
        input.add(multiPartManifestFile.write(run3Entries).get(0));

        // Run4
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r4a-p10", 10, 5, 30),
                                        makeMultiPartEntry(true, "r4a-p11", 20, 11, 31),
                                        makeMultiPartEntry(true, "r4a-p12", 30, 12, 32),
                                        makeMultiPartEntry(true, "r4a-p13", 40, 13, 33),
                                        makeMultiPartEntry(true, "r4a-p14", 50, 14, 34)))
                        .get(0));

        // Run5
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r5a-p11", 11, 8, 40),
                                        makeMultiPartEntry(true, "r5a-p12", 21, 12, 41),
                                        makeMultiPartEntry(true, "r5a-p13", 31, 13, 42),
                                        makeMultiPartEntry(true, "r5a-p14", 41, 14, 43),
                                        makeMultiPartEntry(true, "r5a-p15", 51, 15, 44)))
                        .get(0));

        // Run6
        input.add(
                multiPartManifestFile
                        .write(
                                Arrays.asList(
                                        makeMultiPartEntry(true, "r6a-p7", 7, 4, 50),
                                        makeMultiPartEntry(true, "r6a-p8", 17, 8, 51),
                                        makeMultiPartEntry(true, "r6a-p9", 27, 9, 52),
                                        makeMultiPartEntry(true, "r6a-p10", 37, 10, 53),
                                        makeMultiPartEntry(true, "r6a-p11", 47, 11, 54),
                                        makeMultiPartEntry(true, "r6a-p12", 57, 12, 55)))
                        .get(0));

        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");
        // Sort by the second partition field "f1" (dt)
        testOptions.set("manifest-sort.partition-field", "f1");
        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        multiPartManifestFile,
                        multiPartitionType,
                        CoreOptions.fromMap(testOptions.toMap()));

        // Verify no data loss
        List<ManifestEntry> inputEntries =
                input.stream()
                        .flatMap(
                                f ->
                                        multiPartManifestFile.read(f.fileName(), f.fileSize())
                                                .stream())
                        .collect(Collectors.toList());
        List<String> entryBeforeMerge =
                FileEntry.mergeEntries(inputEntries).stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .map(entry -> entry.kind() + "-" + entry.file().fileName())
                        .collect(Collectors.toList());
        List<String> entryAfterMerge = new ArrayList<>();
        for (ManifestFileMeta meta : merged) {
            for (ManifestEntry entry :
                    multiPartManifestFile.read(meta.fileName(), meta.fileSize())) {
                entryAfterMerge.add(entry.kind() + "-" + entry.file().fileName());
            }
        }
        assertThat(entryBeforeMerge).hasSameElementsAs(entryAfterMerge);

        // Verify entries within each output manifest are sorted by the second field (dt)
        for (ManifestFileMeta meta : merged) {
            List<ManifestEntry> entries =
                    multiPartManifestFile.read(meta.fileName(), meta.fileSize());
            for (int i = 1; i < entries.size(); i++) {
                int prevDt = entries.get(i - 1).partition().getInt(1);
                int currDt = entries.get(i).partition().getInt(1);
                assertThat(currDt)
                        .as("Entries within manifest should be sorted by partition")
                        .isGreaterThanOrEqualTo(prevDt);
            }
        }
    }

    /**
     * Test that when manifest-sort.max-rewrite-size budget is exceeded in the middle of a section,
     * the remaining files are appended to the tail and the final manifest order is preserved.
     *
     * <p>Design:
     *
     * <pre>
     *   - Create a large section with overlapping partition ranges that exceeds the budget
     *   - Set a small manifest-sort.max-rewrite-size to force budget split
     *   - Verify that after merge, all manifests are globally sorted by partition field
     *   - Verify that entries are equivalent (no data loss)
     * </pre>
     */
    @Test
    public void testManifestSortBudgetSplitPreservesOrder() {
        // Create manifests with overlapping ranges, large enough to exceed budget
        List<ManifestFileMeta> input = new ArrayList<>();

        // Manifest A: partitions [0, 10] - large size
        List<ManifestEntry> entriesA = new ArrayList<>();
        for (int p = 0; p <= 10; p++) {
            entriesA.add(makeEntry(true, String.format("A-p%d", p), p));
        }
        ManifestFileMeta manifestA = makeManifest(entriesA.toArray(new ManifestEntry[0]));
        // Manually increase file size to simulate large manifest
        input.add(
                new ManifestFileMeta(
                        manifestA.fileName(),
                        100,
                        manifestA.numAddedFiles(),
                        manifestA.numDeletedFiles(),
                        manifestA.partitionStats(),
                        manifestA.schemaId(),
                        manifestA.minBucket(),
                        manifestA.maxBucket(),
                        manifestA.minLevel(),
                        manifestA.maxLevel(),
                        manifestA.minRowId(),
                        manifestA.maxRowId()));

        // Manifest B: partitions [5, 15] - overlaps with A
        List<ManifestEntry> entriesB = new ArrayList<>();
        for (int p = 5; p <= 15; p++) {
            entriesB.add(makeEntry(true, String.format("B-p%d", p), p));
        }
        ManifestFileMeta manifestB = makeManifest(entriesB.toArray(new ManifestEntry[0]));
        input.add(
                new ManifestFileMeta(
                        manifestB.fileName(),
                        100,
                        manifestB.numAddedFiles(),
                        manifestB.numDeletedFiles(),
                        manifestB.partitionStats(),
                        manifestB.schemaId(),
                        manifestB.minBucket(),
                        manifestB.maxBucket(),
                        manifestB.minLevel(),
                        manifestB.maxLevel(),
                        manifestB.minRowId(),
                        manifestB.maxRowId()));

        // Manifest C: partitions [10, 20] - overlaps with B
        List<ManifestEntry> entriesC = new ArrayList<>();
        for (int p = 10; p <= 20; p++) {
            entriesC.add(makeEntry(true, String.format("C-p%d", p), p));
        }
        ManifestFileMeta manifestC = makeManifest(entriesC.toArray(new ManifestEntry[0]));
        input.add(
                new ManifestFileMeta(
                        manifestC.fileName(),
                        100,
                        manifestC.numAddedFiles(),
                        manifestC.numDeletedFiles(),
                        manifestC.partitionStats(),
                        manifestC.schemaId(),
                        manifestC.minBucket(),
                        manifestC.maxBucket(),
                        manifestC.minLevel(),
                        manifestC.maxLevel(),
                        manifestC.minRowId(),
                        manifestC.maxRowId()));

        // Set small budget to force split
        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");
        testOptions.set("manifest-sort.max-rewrite-size", "150B"); // Total input size is 300B

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        // Verify entries are equivalent
        assertEquivalentEntries(input, merged);

        // Verify global ordering: all manifests sorted by partition min value
        for (int i = 1; i < merged.size(); i++) {
            BinaryRow prevMin = merged.get(i - 1).partitionStats().minValues();
            BinaryRow currMin = merged.get(i).partitionStats().minValues();
            assertThat(currMin.getInt(0))
                    .as("Manifests should be globally sorted by partition field")
                    .isGreaterThanOrEqualTo(prevMin.getInt(0));
        }

        // Verify entries within each manifest are sorted
        for (ManifestFileMeta meta : merged) {
            List<ManifestEntry> entries = manifestFile.read(meta.fileName(), meta.fileSize());
            for (int i = 1; i < entries.size(); i++) {
                int prevPartition = entries.get(i - 1).partition().getInt(0);
                int currPartition = entries.get(i).partition().getInt(0);
                assertThat(currPartition)
                        .as("Entries within manifest should be sorted by partition")
                        .isGreaterThanOrEqualTo(prevPartition);
            }
        }
    }

    /**
     * Test boundary equality (min == previous.max) handling in both SortedRun construction and
     * Section splitting. Boundary-touching files should be allowed in the same SortedRun but may be
     * separated into different Sections.
     *
     * <p>Design:
     *
     * <pre>
     *   - Create manifests with boundary-touching partition ranges
     *   - Manifest A: [0, 5]
     *   - Manifest B: [5, 10] (min == A.max, boundary touching)
     *   - Manifest C: [10, 15] (min == B.max, boundary touching)
     *   - Verify they can be in the same SortedRun (>= comparison)
     *   - Verify they may be split into different Sections (>= comparison with comment)
     * </pre>
     */
    @Test
    public void testBoundaryEqualityHandling() {
        List<ManifestFileMeta> input = new ArrayList<>();

        // Manifest A: partitions [0, 5]
        List<ManifestEntry> entriesA = new ArrayList<>();
        for (int p = 0; p <= 5; p++) {
            entriesA.add(makeEntry(true, String.format("A-p%d", p), p));
        }
        input.add(makeManifest(entriesA.toArray(new ManifestEntry[0])));

        // Manifest B: partitions [5, 10] - boundary touches A (min == A.max)
        List<ManifestEntry> entriesB = new ArrayList<>();
        for (int p = 5; p <= 10; p++) {
            entriesB.add(makeEntry(true, String.format("B-p%d", p), p));
        }
        input.add(makeManifest(entriesB.toArray(new ManifestEntry[0])));

        // Manifest C: partitions [10, 15] - boundary touches B (min == B.max)
        List<ManifestEntry> entriesC = new ArrayList<>();
        for (int p = 10; p <= 15; p++) {
            entriesC.add(makeEntry(true, String.format("C-p%d", p), p));
        }
        input.add(makeManifest(entriesC.toArray(new ManifestEntry[0])));

        Options testOptions = new Options();
        testOptions.set("manifest-sort.enabled", "true");

        List<ManifestFileMeta> merged =
                ManifestFileMerger.merge(
                        input,
                        manifestFile,
                        getPartitionType(),
                        CoreOptions.fromMap(testOptions.toMap()));

        // Verify entries are equivalent
        assertEquivalentEntries(input, merged);

        // Verify all manifests maintain global sort order
        for (int i = 1; i < merged.size(); i++) {
            BinaryRow prevMin = merged.get(i - 1).partitionStats().minValues();
            BinaryRow prevMax = merged.get(i - 1).partitionStats().maxValues();
            BinaryRow currMin = merged.get(i).partitionStats().minValues();

            // Boundary-touching is allowed: currMin >= prevMin
            assertThat(currMin.getInt(0))
                    .as("Global order should be maintained with boundary-touching allowed")
                    .isGreaterThanOrEqualTo(prevMin.getInt(0));

            // Log boundary equality cases for documentation
            if (currMin.getInt(0) == prevMax.getInt(0)) {
                System.out.println(
                        String.format(
                                "Boundary equality detected: manifest[%d].min=%d == manifest[%d].max=%d",
                                i, currMin.getInt(0), i - 1, prevMax.getInt(0)));
            }
        }

        // Verify entries within each manifest are sorted
        for (ManifestFileMeta meta : merged) {
            List<ManifestEntry> entries = manifestFile.read(meta.fileName(), meta.fileSize());
            for (int i = 1; i < entries.size(); i++) {
                int prevPartition = entries.get(i - 1).partition().getInt(0);
                int currPartition = entries.get(i).partition().getInt(0);
                assertThat(currPartition)
                        .as("Entries within manifest should be sorted by partition")
                        .isGreaterThanOrEqualTo(prevPartition);
            }
        }
    }

    /** Create a ManifestEntry with a 3-field partition row (region, dt, hour). */
    private ManifestEntry makeMultiPartEntry(
            boolean isAdd, String fileName, int region, int dt, int hour) {
        BinaryRow binaryRow = new BinaryRow(3);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, region);
        writer.writeInt(1, dt);
        writer.writeInt(2, hour);
        writer.complete();

        return ManifestEntry.create(
                isAdd ? FileKind.ADD : FileKind.DELETE,
                binaryRow,
                0,
                0,
                DataFileMeta.create(
                        fileName,
                        0,
                        0,
                        binaryRow,
                        binaryRow,
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
                        null,
                        null));
    }
}
