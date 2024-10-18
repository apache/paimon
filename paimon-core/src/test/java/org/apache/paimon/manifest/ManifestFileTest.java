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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.StatsTestUtils;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.paimon.TestKeyValueGenerator.DEFAULT_PART_TYPE;
import static org.apache.paimon.stats.StatsTestUtils.convertWithoutSchemaEvolution;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFile}. */
public class ManifestFileTest {

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();
    private final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadManifestFile() {
        List<ManifestEntry> entries = generateData();
        ManifestFileMeta meta = gen.createManifestFileMeta(entries);
        ManifestFile manifestFile = createManifestFile(tempDir.toString());

        List<ManifestFileMeta> actualMetas = manifestFile.write(entries);
        checkRollingFiles(meta, actualMetas, manifestFile.suggestedFileSize());
        List<ManifestEntry> actualEntries =
                actualMetas.stream()
                        .flatMap(m -> manifestFile.read(m.fileName(), m.fileSize()).stream())
                        .collect(Collectors.toList());
        assertThat(actualEntries).isEqualTo(entries);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 1, 10);
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile =
                createManifestFile(FailingFileIO.getFailingPath(failingName, tempDir.toString()));

        try {
            manifestFile.write(entries);
        } catch (Throwable e) {
            assertThat(e).hasRootCauseExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            assertThat(LocalFileIO.create().listStatus(manifestDir)).isEmpty();
        }
    }

    private List<ManifestEntry> generateData() {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(gen.next());
        }
        return entries;
    }

    private ManifestFile createManifestFile(String pathStr) {
        Path path = new Path(pathStr);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGCY_NAME.defaultValue());
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        FileIO fileIO = FileIOFinder.find(path);
        return new ManifestFile.Factory(
                        fileIO,
                        new SchemaManager(fileIO, path),
                        DEFAULT_PART_TYPE,
                        avro,
                        "zstd",
                        pathFactory,
                        suggestedFileSize,
                        null)
                .create();
    }

    private void checkRollingFiles(
            ManifestFileMeta expected, List<ManifestFileMeta> actual, long suggestedFileSize) {
        // all but last file should be no smaller than suggestedFileSize
        for (int i = 0; i + 1 < actual.size(); i++) {
            assertThat(actual.get(i).fileSize() >= suggestedFileSize).isTrue();
        }

        // expected.numAddedFiles == sum(numAddedFiles)
        assertThat(actual.stream().mapToLong(ManifestFileMeta::numAddedFiles).sum())
                .isEqualTo(expected.numAddedFiles());

        // expected.numDeletedFiles == sum(numDeletedFiles)
        assertThat(actual.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum())
                .isEqualTo(expected.numDeletedFiles());

        // check stats
        SimpleColStats[] fieldStats =
                convertWithoutSchemaEvolution(expected.partitionStats(), DEFAULT_PART_TYPE);
        for (int i = 0; i < fieldStats.length; i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    fieldStats[i],
                    actual,
                    meta ->
                            convertWithoutSchemaEvolution(meta.partitionStats(), DEFAULT_PART_TYPE)[
                                    idx]);
        }
    }
}
