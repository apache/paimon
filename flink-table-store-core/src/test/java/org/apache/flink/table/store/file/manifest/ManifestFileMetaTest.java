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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.StatsTestUtils;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFileMeta}. */
public class ManifestFileMetaTest {

    private static final RowType PARTITION_TYPE = RowType.of(new IntType());
    private static final RowType KEY_TYPE = RowType.of(new IntType());
    private static final RowType ROW_TYPE = RowType.of(new BigIntType());

    private final FileFormat avro;

    @TempDir java.nio.file.Path tempDir;
    private ManifestFile manifestFile;

    public ManifestFileMetaTest() {
        this.avro =
                FileFormat.fromIdentifier(
                        ManifestFileMetaTest.class.getClassLoader(), "avro", new Configuration());
    }

    @BeforeEach
    public void beforeEach() {
        manifestFile = createManifestFile(tempDir.toString());
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4})
    public void testMerge(int numLastBits) {
        List<ManifestFileMeta> input = new ArrayList<>();
        List<ManifestFileMeta> expected = new ArrayList<>();
        createData(numLastBits, input, expected);

        List<ManifestFileMeta> actual = ManifestFileMeta.merge(input, manifestFile, 500, 3);
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

    private void assertSameContent(
            ManifestFileMeta expected, ManifestFileMeta actual, ManifestFile manifestFile) {
        // check meta
        assertThat(actual.numAddedFiles()).isEqualTo(expected.numAddedFiles());
        assertThat(actual.numDeletedFiles()).isEqualTo(expected.numDeletedFiles());
        assertThat(actual.partitionStats()).isEqualTo(expected.partitionStats());

        // check content
        assertThat(manifestFile.read(actual.fileName()))
                .isEqualTo(manifestFile.read(expected.fileName()));
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        FailingAtomicRenameFileSystem.get().reset(1, 10);
        List<ManifestFileMeta> input = new ArrayList<>();
        createData(ThreadLocalRandom.current().nextInt(5), input, null);
        ManifestFile failingManifestFile =
                createManifestFile(
                        FailingAtomicRenameFileSystem.getFailingPath(tempDir.toString()));

        try {
            ManifestFileMeta.merge(input, failingManifestFile, 500, 3);
        } catch (Throwable e) {
            assertThat(e)
                    .hasRootCauseExactlyInstanceOf(
                            FailingAtomicRenameFileSystem.ArtificialException.class);
            // old files should be kept untouched, while new files should be cleaned up
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            FileSystem fs = manifestDir.getFileSystem();
            assertThat(
                            new TreeSet<>(
                                    Arrays.stream(fs.listStatus(manifestDir))
                                            .map(s -> s.getPath().getName())
                                            .collect(Collectors.toList())))
                    .isEqualTo(
                            new TreeSet<>(
                                    input.stream()
                                            .map(ManifestFileMeta::fileName)
                                            .collect(Collectors.toList())));
        }
    }

    private ManifestFile createManifestFile(String path) {
        return new ManifestFile.Factory(
                        PARTITION_TYPE,
                        avro,
                        new FileStorePathFactory(
                                new Path(path),
                                PARTITION_TYPE,
                                "default",
                                FileStoreOptions.FILE_FORMAT.defaultValue()),
                        Long.MAX_VALUE)
                .create();
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

    private ManifestFileMeta makeManifest(ManifestEntry... entries) {
        ManifestFileMeta writtenMeta = manifestFile.write(Arrays.asList(entries)).get(0);
        return new ManifestFileMeta(
                writtenMeta.fileName(),
                entries.length * 100, // for testing purpose
                writtenMeta.numAddedFiles(),
                writtenMeta.numDeletedFiles(),
                writtenMeta.partitionStats());
    }

    private ManifestEntry makeEntry(boolean isAdd, String fileName) {
        BinaryRowData binaryRowData = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRowData);
        writer.writeInt(0, 0);
        writer.complete();

        return new ManifestEntry(
                isAdd ? ValueKind.ADD : ValueKind.DELETE,
                binaryRowData, // not used
                0, // not used
                0, // not used
                new DataFileMeta(
                        fileName,
                        0, // not used
                        0, // not used
                        binaryRowData, // not useds
                        binaryRowData, // not used
                        StatsTestUtils.newEmptyTableStats(), // not used
                        StatsTestUtils.newEmptyTableStats(), // not used
                        0, // not used
                        0, // not used
                        0 // not used
                        ));
    }
}
