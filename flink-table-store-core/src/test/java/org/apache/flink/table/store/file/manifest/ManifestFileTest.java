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
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        ManifestFileMeta actualMeta = manifestFile.write(entries);
        // we do not check file name and size as we can't know in advance
        checkMetaIgnoringFileNameAndSize(meta, actualMeta);
        List<ManifestEntry> actualEntries = manifestFile.read(actualMeta.fileName());
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
                        new Path(path), TestKeyValueGenerator.PARTITION_TYPE, "default");
        return new ManifestFile.Factory(
                        TestKeyValueGenerator.PARTITION_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        avro,
                        pathFactory)
                .create();
    }

    private void checkMetaIgnoringFileNameAndSize(
            ManifestFileMeta expected, ManifestFileMeta actual) {
        assertThat(actual.numAddedFiles()).isEqualTo(expected.numAddedFiles());
        assertThat(actual.numDeletedFiles()).isEqualTo(expected.numDeletedFiles());
        assertThat(actual.partitionStats()).isEqualTo(expected.partitionStats());
    }
}
