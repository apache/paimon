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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.format.avro.AvroBlockReader;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that Iceberg metadata blocks are independent of data-file block sizing. */
class IcebergManifestBlockSizeTest {

    private static final int NUM_RECORDS = 10_000;

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"1 kb, 2", "256 mb, 2", "1 kb, 3", "256 mb, 3"})
    void testManifestIgnoresDataFileBlockSize(String blockSize, int formatVersion)
            throws Exception {
        FileStoreTable table = createTable(blockSize, formatVersion);
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestFile manifest = IcebergManifestFile.create(table, pathFactory);
        GenericMap emptyMap = new GenericMap(Collections.emptyMap());
        IcebergDataFileMeta dataFile =
                new IcebergDataFileMeta(
                        IcebergDataFileMeta.Content.DATA,
                        "data.parquet",
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        1,
                        100,
                        emptyMap,
                        emptyMap,
                        emptyMap);
        List<IcebergManifestEntry> entries =
                Collections.nCopies(
                        NUM_RECORDS,
                        new IcebergManifestEntry(
                                IcebergManifestEntry.Status.ADDED, 1, 1, 1, dataFile));

        List<IcebergManifestFileMeta> files = manifest.rollingWrite(entries.iterator(), 1);

        assertThat(files).hasSize(1);
        assertDefaultBlocks(table, new Path(files.get(0).manifestPath()), "manifest_entry");
        assertThat(manifest.read(files.get(0)))
                .hasSize(NUM_RECORDS)
                .allSatisfy(
                        entry -> {
                            assertThat(entry.status()).isEqualTo(IcebergManifestEntry.Status.ADDED);
                            assertThat(entry.file().filePath()).isEqualTo("data.parquet");
                            assertThat(entry.file().recordCount()).isEqualTo(1);
                        });
        assertThat(table.coreOptions().toConfiguration().get(CoreOptions.FILE_BLOCK_SIZE))
                .isEqualTo(MemorySize.parse(blockSize));
    }

    @ParameterizedTest
    @CsvSource({"1 kb, 2", "256 mb, 2", "1 kb, 3", "256 mb, 3"})
    void testManifestListIgnoresDataFileBlockSize(String blockSize, int formatVersion)
            throws Exception {
        FileStoreTable table = createTable(blockSize, formatVersion);
        IcebergPathFactory pathFactory =
                new IcebergPathFactory(new Path(table.location(), "metadata"));
        IcebergManifestList manifestList = IcebergManifestList.create(table, pathFactory);
        List<IcebergManifestFileMeta> files =
                Collections.nCopies(
                        NUM_RECORDS,
                        new IcebergManifestFileMeta(
                                "manifest.avro",
                                100,
                                0,
                                IcebergManifestFileMeta.Content.DATA,
                                1,
                                1,
                                1,
                                1,
                                0,
                                0,
                                1,
                                0,
                                0,
                                Collections.emptyList(),
                                null));

        String fileName = manifestList.writeWithoutRolling(files);

        assertDefaultBlocks(table, pathFactory.toManifestListPath(fileName), "manifest_file");
        assertThat(manifestList.read(fileName)).containsExactlyElementsOf(files);
        assertThat(table.coreOptions().toConfiguration().get(CoreOptions.FILE_BLOCK_SIZE))
                .isEqualTo(MemorySize.parse(blockSize));
    }

    private FileStoreTable createTable(String blockSize, int formatVersion) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, "parquet");
        options.set(CoreOptions.FILE_BLOCK_SIZE, MemorySize.parse(blockSize));
        options.set(IcebergOptions.FORMAT_VERSION, formatVersion);
        options.set(IcebergOptions.MANIFEST_COMPRESSION, "deflate");
        Schema schema =
                new Schema(
                        DataTypes.ROW(DataTypes.INT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options.toMap(),
                        "");
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toUri());
        new FileSystemSchemaManager(fileIO, path).createTable(schema);
        return FileStoreTableFactory.create(fileIO, path);
    }

    private void assertDefaultBlocks(FileStoreTable table, Path path, String recordName)
            throws IOException {
        try (DataFileStream<GenericRecord> reader =
                new DataFileStream<>(
                        table.fileIO().newInputStream(path), new GenericDatumReader<>())) {
            assertThat(reader.getSchema().getName()).isEqualTo(recordName);
            assertThat(reader.getMetaString("avro.codec")).isEqualTo("deflate");
        }
        long records = 0;
        int blocks = 0;
        try (AvroBlockReader reader = new AvroBlockReader(table.fileIO().newInputStream(path))) {
            while (reader.hasNextBlock()) {
                AvroRawBlock block = reader.nextBorrowedRawBlock();
                int size = block.decompress(null).remaining();
                // The threshold is checked after each record, so a block can exceed it slightly.
                assertThat(size).isLessThan(DEFAULT_SYNC_INTERVAL + 1024);
                if (reader.hasNextBlock()) {
                    assertThat(size).isGreaterThanOrEqualTo(DEFAULT_SYNC_INTERVAL);
                }
                records += block.recordCount();
                blocks++;
            }
        }
        assertThat(blocks).isGreaterThan(1);
        assertThat(records).isEqualTo(NUM_RECORDS);
    }
}
