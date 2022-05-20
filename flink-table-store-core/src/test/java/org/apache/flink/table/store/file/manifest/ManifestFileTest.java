/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.StatsTestUtils;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFile}. */
public class ManifestFileTest {

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();
    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    ManifestFileTest.class.getClassLoader(), "avro", new Configuration());

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
                        .flatMap(m -> manifestFile.read(m.fileName()).stream())
                        .collect(Collectors.toList());
        assertThat(actualEntries).isEqualTo(entries);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        FailingAtomicRenameFileSystem.get().reset(1, 10);
        List<ManifestEntry> entries = generateData();
        ManifestFile manifestFile =
                createManifestFile(
                        FailingAtomicRenameFileSystem.getFailingPath(tempDir.toString()));

        try {
            manifestFile.write(entries);
        } catch (Throwable e) {
            assertThat(e)
                    .hasRootCauseExactlyInstanceOf(
                            FailingAtomicRenameFileSystem.ArtificialException.class);
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            FileSystem fs = manifestDir.getFileSystem();
            assertThat(fs.listStatus(manifestDir)).isEmpty();
        }
    }

    private List<ManifestEntry> generateData() {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(gen.next());
        }
        return entries;
    }

    private ManifestFile createManifestFile(String path) {
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        new Path(path),
                        TestKeyValueGenerator.PARTITION_TYPE,
                        "default",
                        FileStoreOptions.FILE_FORMAT.defaultValue());
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        return new ManifestFile.Factory(
                        TestKeyValueGenerator.PARTITION_TYPE, avro, pathFactory, suggestedFileSize)
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
        for (int i = 0; i < expected.partitionStats().fields(null).length; i++) {
            int idx = i;
            StatsTestUtils.checkRollingFileStats(
                    expected.partitionStats().fields(null)[i],
                    actual,
                    meta -> meta.partitionStats().fields(null)[idx]);
        }
    }
}
