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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Tests for writer metadata based stats extraction in Mosaic format. */
class MosaicWriterMetadataTest {

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Test
    void testWriterMetadataNotNull() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        FormatWriter writer = createWriter(rowType, path);
        writer.addElement(GenericRow.of(1, BinaryString.fromString("hello")));
        writer.addElement(GenericRow.of(2, BinaryString.fromString("world")));
        writer.close();

        Object metadata = writer.writerMetadata();
        assertThat(metadata).isNotNull();
        assertThat(metadata).isInstanceOf(MosaicWriterMetadata.class);

        MosaicWriterMetadata mosaicMeta = (MosaicWriterMetadata) metadata;
        assertThat(mosaicMeta.numRowGroups()).isGreaterThan(0);
    }

    @Test
    void testStatsFromMetadataMatchesStatsFromFile() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_bigint", DataTypes.BIGINT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        Path path = newPath();

        FormatWriter writer = createWriter(rowType, path);
        for (int i = 0; i < 1000; i++) {
            writer.addElement(
                    GenericRow.of(i, (long) i * 100, BinaryString.fromString("val_" + i), i * 1.1));
        }
        writer.close();

        Object metadata = writer.writerMetadata();
        assertThat(metadata).isNotNull();

        MosaicFileFormat format = createFormat();
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromFile = extractor.extract(fileIO, path, fileSize);
        SimpleColStats[] fromMetadata = extractor.extract(fileIO, path, fileSize, metadata);

        assertThat(fromMetadata).isEqualTo(fromFile);
    }

    @Test
    void testStatsFromMetadataWithNullValues() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();

        FormatWriter writer = createWriter(rowType, path);
        writer.addElement(GenericRow.of(1, null));
        writer.addElement(GenericRow.of(null, BinaryString.fromString("a")));
        writer.addElement(GenericRow.of(3, BinaryString.fromString("b")));
        writer.close();

        Object metadata = writer.writerMetadata();
        MosaicFileFormat format = createFormat();
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromMetadata = extractor.extract(fileIO, path, fileSize, metadata);
        assertThat(fromMetadata).isNotNull();
        assertThat(fromMetadata[0].nullCount()).isEqualTo(1L);
        assertThat(fromMetadata[1].nullCount()).isEqualTo(1L);
    }

    @Test
    void testFallbackToFileWhenMetadataIsNull() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        FormatWriter writer = createWriter(rowType, path);
        writer.addElement(GenericRow.of(10, BinaryString.fromString("test")));
        writer.close();

        MosaicFileFormat format = createFormat();
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromFile = extractor.extract(fileIO, path, fileSize);
        SimpleColStats[] fromNull = extractor.extract(fileIO, path, fileSize, null);

        assertThat(fromNull).isEqualTo(fromFile);
    }

    private Path newPath() {
        return new Path(tempDir.toUri().toString(), UUID.randomUUID() + ".mosaic");
    }

    private FormatWriter createWriter(RowType rowType, Path path) throws IOException {
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();
        return writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
    }

    private static MosaicFileFormat createFormat() {
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
