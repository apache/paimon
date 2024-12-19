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
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.TablePathProvider;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestList}. */
public class ManifestListTest {

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();
    private final FileFormat avro = FileFormat.fromIdentifier("avro", new Options());

    @TempDir java.nio.file.Path tempDir;

    @RepeatedTest(10)
    public void testWriteAndReadManifestList() {
        List<ManifestFileMeta> metas = generateData();
        ManifestList manifestList = createManifestList(tempDir.toString());

        String manifestListName = manifestList.write(metas);
        List<ManifestFileMeta> actualMetas = manifestList.read(manifestListName);
        assertThat(actualMetas).isEqualTo(metas);
    }

    @RepeatedTest(10)
    public void testCleanUpForException() throws IOException {
        String failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 1, 3);
        List<ManifestFileMeta> metas = generateData();
        ManifestList manifestList =
                createManifestList(FailingFileIO.getFailingPath(failingName, tempDir.toString()));

        try {
            manifestList.write(metas);
        } catch (Throwable e) {
            assertThat(e).hasRootCauseExactlyInstanceOf(FailingFileIO.ArtificialException.class);
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            assertThat(LocalFileIO.create().listStatus(manifestDir)).isEmpty();
        }
    }

    @Test
    public void testManifestListNaming() {
        List<ManifestFileMeta> metas = generateData();
        ManifestList manifestList = createManifestList(tempDir.toString());

        String manifestListName = manifestList.write(metas);
        assertThat(manifestListName.startsWith("manifest-list-")).isTrue();
    }

    private List<ManifestFileMeta> generateData() {
        Random random = new Random();
        List<ManifestFileMeta> metas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            List<ManifestEntry> entries = new ArrayList<>();
            for (int j = random.nextInt(10) + 1; j > 0; j--) {
                entries.add(gen.next());
            }
            metas.add(gen.createManifestFileMeta(entries));
        }
        return metas;
    }

    private ManifestList createManifestList(String pathStr) {
        Path path = new Path(pathStr);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        new TablePathProvider(path),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGCY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        return new ManifestList.Factory(FileIOFinder.find(path), avro, "zstd", pathFactory, null)
                .create();
    }
}
