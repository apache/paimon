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

import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.table.BucketMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for encoded index manifest Avro reads and writes. */
class IndexManifestAvroReaderWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testEncodedRecordsAndRawBlocksRoundTrip() throws Exception {
        TestAppendFileStore fileStore =
                TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        IndexManifestFile indexManifestFile =
                new IndexManifestFile.Factory(
                                fileStore.fileIO(),
                                FileFormat.manifestFormat(fileStore.options()),
                                "zstd",
                                fileStore.pathFactory(),
                                null)
                        .create();

        List<IndexManifestEntry> expected = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            expected.add(entry(i));
        }
        String source =
                indexManifestFile.writeIndexFiles(null, expected, BucketMode.BUCKET_UNAWARE);
        List<IndexManifestEntry> sourceEntries = indexManifestFile.read(source);

        int blockCount = 0;
        IndexManifestAvroWriter writer = indexManifestFile.createAvroWriter();
        try (IndexManifestAvroReader reader = indexManifestFile.scanAvroBlocks(source)) {
            assertThat(reader.rawBlockCopySupported()).isTrue();
            while (reader.hasNext()) {
                IndexManifestAvroReader.RawBlock block = reader.next();
                if (blockCount++ == 0) {
                    IndexManifestAvroReader.RowIterator rows =
                            block.toRows(IndexManifestEntry.MANIFEST_ROW_TYPE);
                    while (rows.hasNext()) {
                        rows.next();
                        writer.writeEncoded(rows.encodedRecord());
                    }
                } else {
                    writer.writeEncodedBlock(block.encodedBlock());
                }
            }
            writer.close();
        } catch (Exception | Error failure) {
            writer.abort(failure);
            throw failure;
        }

        assertThat(blockCount).isGreaterThan(1);
        assertThat(indexManifestFile.read(writer.result()))
                .containsExactlyElementsOf(sourceEntries);
    }

    private IndexManifestEntry entry(int index) {
        long start = index * 100L;
        byte[] indexMeta = new byte[8 * 1024];
        indexMeta[0] = (byte) index;
        return new IndexManifestEntry(
                FileKind.ADD,
                BinaryRow.EMPTY_ROW,
                0,
                new IndexFileMeta(
                        "btree",
                        "index-" + index,
                        1L,
                        100L,
                        new GlobalIndexMeta(start, start + 99, 1, null, indexMeta),
                        null));
    }
}
