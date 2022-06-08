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
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestList}. */
public class ManifestListTest {

    private final ManifestTestDataGenerator gen = ManifestTestDataGenerator.builder().build();
    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    ManifestListTest.class.getClassLoader(), "avro", new Configuration());

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
        FailingAtomicRenameFileSystem.get().reset(1, 3);
        List<ManifestFileMeta> metas = generateData();
        ManifestList manifestList =
                createManifestList(
                        FailingAtomicRenameFileSystem.getFailingPath(tempDir.toString()));

        try {
            manifestList.write(metas);
        } catch (Throwable e) {
            assertThat(e)
                    .hasRootCauseExactlyInstanceOf(
                            FailingAtomicRenameFileSystem.ArtificialException.class);
            Path manifestDir = new Path(tempDir.toString() + "/manifest");
            FileSystem fs = manifestDir.getFileSystem();
            assertThat(fs.listStatus(manifestDir)).isEmpty();
        }
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

    private ManifestList createManifestList(String path) {
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        new Path(path),
                        TestKeyValueGenerator.PARTITION_TYPE,
                        "default",
                        FileStoreOptions.FILE_FORMAT.defaultValue());
        return new ManifestList.Factory(TestKeyValueGenerator.PARTITION_TYPE, avro, pathFactory)
                .create();
    }
}
