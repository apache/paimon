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

package org.apache.paimon.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IndexFilePathFactories;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link IndexFileHandler#deleteIndexFile} and {@link IndexFileHandler#existsIndexFile}.
 */
public class IndexFileHandlerTest {

    @TempDir java.nio.file.Path tempPath;

    private FileIO fileIO;
    private IndexFileHandler handler;

    @BeforeEach
    void setUp() {
        fileIO = LocalFileIO.create();
        Path root = new Path(tempPath.toUri());

        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        root,
                        RowType.builder().build(),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
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
                        null);

        handler =
                new IndexFileHandler(
                        fileIO,
                        null,
                        null,
                        new IndexFilePathFactories(pathFactory),
                        MemorySize.ofMebiBytes(2),
                        false);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "HASH",
                "DELETION_VECTORS",
                "btree",
                "bitmap",
                "lumina",
                "lumina-vector-ann"
            })
    void testExistsAndDeleteIndexFile(String indexType) throws IOException {
        String fileName = "index-" + UUID.randomUUID();
        IndexFileMeta meta =
                new IndexFileMeta(indexType, fileName, 4, 1, (GlobalIndexMeta) null, null);
        IndexManifestEntry entry =
                new IndexManifestEntry(FileKind.ADD, BinaryRow.EMPTY_ROW, 0, meta);

        // create the file at the expected path
        Path filePath = handler.filePath(entry);
        fileIO.tryToWriteAtomic(filePath, "test");

        assertThat(handler.existsIndexFile(entry)).isTrue();

        handler.deleteIndexFile(entry);

        assertThat(handler.existsIndexFile(entry)).isFalse();
    }
}
