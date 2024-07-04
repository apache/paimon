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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;

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
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

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
        List<ManifestFileMeta> actual =
                ManifestFileMeta.merge(
                        input, manifestFile, 500, 3, Long.MAX_VALUE, getPartitionType(), null);
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
            ManifestFileMeta.merge(
                    input,
                    failingManifestFile,
                    500,
                    3,
                    fullCompactionThreshold,
                    getPartitionType(),
                    null);
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
        testCleanUp(input, 0L);
    }

    @Test
    public void testMerge() {
        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);
        // delta with delete apply parititon 1,2
        addDeltaManifests(input, true);
        // trigger full compaction
        List<ManifestFileMeta> merged =
                ManifestFileMeta.merge(input, manifestFile, 500, 3, 200, getPartitionType(), null);

        // 1st Manifest don't need to Merge
        assertSameContent(input.get(0), merged.get(0), manifestFile);

        // after merge the entry is equivalent
        assertEquivalentEntries(input, merged);
    }

    @Test
    public void testMergeWithoutDelta() {

        // base
        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);

        List<ManifestFileMeta> merged =
                ManifestFileMeta.merge(input, manifestFile, 500, 3, 200, getPartitionType(), null);

        assertEquivalentEntries(input, merged);
        assertThat(merged).hasSameElementsAs(input);

        // Delete and Add are completely neutralized in  delta.
        List<ManifestFileMeta> base = createBaseManifestFileMetas(true);
        List<ManifestFileMeta> input1 = new ArrayList<>(base);
        ManifestFileMeta delta = makeManifest(makeEntry(true, "A", 1), makeEntry(false, "A", 1));
        input1.add(delta);

        List<ManifestFileMeta> merged1 =
                ManifestFileMeta.merge(input1, manifestFile, 500, 3, 200, getPartitionType(), null);

        assertThat(base).hasSameElementsAs(merged1);
        assertEquivalentEntries(input1, merged1);
    }

    @Test
    public void testMergeWithoutBase() {
        List<ManifestFileMeta> input = new ArrayList<>();
        addDeltaManifests(input, true);
        List<ManifestFileMeta> merged =
                ManifestFileMeta.merge(input, manifestFile, 500, 3, 200, getPartitionType(), null);
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

        List<ManifestFileMeta> merged =
                ManifestFileMeta.merge(input, manifestFile, 500, 3, 200, getPartitionType(), null);
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
        input.add(makeManifest(entries.toArray(new ManifestEntry[0])));

        // delta manifest
        input.add(makeManifest(makeEntry(true, "A"), makeEntry(true, "B"), makeEntry(true, "C")));
        input.add(makeManifest(makeEntry(true, "D")));
        input.add(makeManifest(makeEntry(false, "A"), makeEntry(false, "B"), makeEntry(true, "F")));
        input.add(makeManifest(makeEntry(false, "14"), makeEntry(false, "15")));
        input.add(
                makeManifest(
                        makeEntry(false, "C"),
                        makeEntry(false, "D"),
                        makeEntry(false, "F"),
                        makeEntry(true, "G")));

        // no trigger for delta size
        List<ManifestFileMeta> newMetas2 = new ArrayList<>();
        Optional<List<ManifestFileMeta>> fullCompacted =
                ManifestFileMeta.tryFullCompaction(
                        input,
                        newMetas2,
                        manifestFile,
                        500,
                        Long.MAX_VALUE,
                        getPartitionType(),
                        null);
        assertThat(fullCompacted).isEmpty();
        assertThat(newMetas2).isEmpty();

        // trigger full compaction
        List<ManifestFileMeta> newMetas3 = new ArrayList<>();
        List<ManifestFileMeta> merged =
                ManifestFileMeta.tryFullCompaction(
                                input, newMetas3, manifestFile, 500, 100, getPartitionType(), null)
                        .get();

        List<String> entryFileNameExptected = new ArrayList<>();
        entryFileNameExptected.add("ADD-G");
        for (int i = 0; i < 14; i++) {
            entryFileNameExptected.add("ADD-" + i);
        }

        containSameEntryFile(merged, entryFileNameExptected);

        assertThat(newMetas3).isNotEmpty();
    }

    @Test
    public void testMultiPartitionsFullCompaction() throws Exception {

        List<ManifestFileMeta> input = createBaseManifestFileMetas(true);

        // delta with delete apply partition 1,2
        addDeltaManifests(input, true);

        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> mergedManifest =
                ManifestFileMeta.tryFullCompaction(
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

        containSameEntryFile(mergedManifest, expected);
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
}
