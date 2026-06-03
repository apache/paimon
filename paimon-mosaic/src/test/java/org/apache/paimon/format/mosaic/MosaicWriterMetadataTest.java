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
import org.apache.paimon.format.SimpleStatsExtractor.FileInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

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

        FormatWriter writer = createWriter(rowType, path, "f0,f1");
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
        String statsColumns = "f_int,f_bigint,f_string,f_double";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        for (int i = 0; i < 1000; i++) {
            writer.addElement(
                    GenericRow.of(i, (long) i * 100, BinaryString.fromString("val_" + i), i * 1.1));
        }
        writer.close();

        Object metadata = writer.writerMetadata();
        assertThat(metadata).isNotNull();

        MosaicFileFormat format = createFormat(statsColumns);
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
        String statsColumns = "f_int,f_string";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        writer.addElement(GenericRow.of(1, null));
        writer.addElement(GenericRow.of(null, BinaryString.fromString("a")));
        writer.addElement(GenericRow.of(3, BinaryString.fromString("b")));
        writer.close();

        Object metadata = writer.writerMetadata();
        MosaicFileFormat format = createFormat(statsColumns);
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
    void testExtractWithFileInfoRowCount() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();
        String statsColumns = "f_int,f_string";

        int numRows = 500;
        FormatWriter writer = createWriter(rowType, path, statsColumns);
        for (int i = 0; i < numRows; i++) {
            writer.addElement(GenericRow.of(i, BinaryString.fromString("row_" + i)));
        }
        writer.close();

        MosaicFileFormat format = createFormat(statsColumns);
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        Pair<SimpleColStats[], FileInfo> result =
                extractor.extractWithFileInfo(fileIO, path, fileSize);
        assertThat(result.getRight().getRowCount()).isEqualTo(numRows);
        assertThat(result.getLeft()).isNotNull();
        assertThat(result.getLeft()).hasSize(fieldCount);
    }

    @Test
    void testPartialStatsColumnsFromMetadata() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        Path path = newPath();
        String statsColumns = "f_int";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        writer.addElement(GenericRow.of(1, BinaryString.fromString("a"), 1.0));
        writer.addElement(GenericRow.of(null, BinaryString.fromString("b"), 2.0));
        writer.addElement(GenericRow.of(3, null, null));
        writer.close();

        Object metadata = writer.writerMetadata();
        assertThat(metadata).isInstanceOf(MosaicWriterMetadata.class);
        MosaicWriterMetadata mosaicMeta = (MosaicWriterMetadata) metadata;
        assertThat(mosaicMeta.statsColumnNames()).containsExactly("f_int");

        MosaicFileFormat format = createFormat(statsColumns);
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromMetadata = extractor.extract(fileIO, path, fileSize, metadata);

        // f_int has stats: min=1, max=3, nullCount=1
        assertThat(fromMetadata[0].min()).isEqualTo(1);
        assertThat(fromMetadata[0].max()).isEqualTo(3);
        assertThat(fromMetadata[0].nullCount()).isEqualTo(1L);

        // f_string and f_double have no stats (not in statsColumns)
        assertThat(fromMetadata[1].min()).isNull();
        assertThat(fromMetadata[1].max()).isNull();
        assertThat(fromMetadata[1].nullCount()).isNull();
        assertThat(fromMetadata[2].min()).isNull();
        assertThat(fromMetadata[2].max()).isNull();
        assertThat(fromMetadata[2].nullCount()).isNull();
    }

    @Test
    void testStatsOnMiddleColumn() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        Path path = newPath();
        String statsColumns = "f_string";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        writer.addElement(GenericRow.of(1, BinaryString.fromString("banana"), 1.0));
        writer.addElement(GenericRow.of(2, BinaryString.fromString("apple"), 2.0));
        writer.addElement(GenericRow.of(3, null, 3.0));
        writer.close();

        Object metadata = writer.writerMetadata();
        MosaicFileFormat format = createFormat(statsColumns);
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromMetadata = extractor.extract(fileIO, path, fileSize, metadata);

        // f_int has no stats
        assertThat(fromMetadata[0].min()).isNull();
        assertThat(fromMetadata[0].max()).isNull();
        assertThat(fromMetadata[0].nullCount()).isNull();

        // f_string has stats: min="apple", max="banana", nullCount=1
        assertThat(fromMetadata[1].min()).isEqualTo(BinaryString.fromString("apple"));
        assertThat(fromMetadata[1].max()).isEqualTo(BinaryString.fromString("banana"));
        assertThat(fromMetadata[1].nullCount()).isEqualTo(1L);

        // f_double has no stats
        assertThat(fromMetadata[2].min()).isNull();
        assertThat(fromMetadata[2].max()).isNull();
        assertThat(fromMetadata[2].nullCount()).isNull();
    }

    @Test
    void testPartialStatsColumnsFromFile() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        Path path = newPath();
        String statsColumns = "f_string";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        writer.addElement(GenericRow.of(1, BinaryString.fromString("banana"), 1.0));
        writer.addElement(GenericRow.of(2, BinaryString.fromString("apple"), 2.0));
        writer.addElement(GenericRow.of(3, null, 3.0));
        writer.close();

        // Extract from file (no writer metadata), simulating fallback path
        MosaicFileFormat format = createFormat(statsColumns);
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] collectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(i -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);

        SimpleStatsExtractor extractor = format.createStatsExtractor(rowType, collectors).get();
        LocalFileIO fileIO = new LocalFileIO();
        long fileSize = fileIO.getFileSize(path);

        SimpleColStats[] fromFile = extractor.extract(fileIO, path, fileSize);

        // f_int has no stats in file
        assertThat(fromFile[0].min()).isNull();
        assertThat(fromFile[0].max()).isNull();
        assertThat(fromFile[0].nullCount()).isNull();

        // f_string has stats
        assertThat(fromFile[1].min()).isEqualTo(BinaryString.fromString("apple"));
        assertThat(fromFile[1].max()).isEqualTo(BinaryString.fromString("banana"));
        assertThat(fromFile[1].nullCount()).isEqualTo(1L);

        // f_double has no stats in file
        assertThat(fromFile[2].min()).isNull();
        assertThat(fromFile[2].max()).isNull();
        assertThat(fromFile[2].nullCount()).isNull();
    }

    @Test
    void testFallbackToFileWhenMetadataIsNull() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();
        String statsColumns = "f0,f1";

        FormatWriter writer = createWriter(rowType, path, statsColumns);
        writer.addElement(GenericRow.of(10, BinaryString.fromString("test")));
        writer.close();

        MosaicFileFormat format = createFormat(statsColumns);
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

    private FormatWriter createWriter(RowType rowType, Path path, String statsColumns)
            throws IOException {
        MosaicFileFormat format = createFormat(statsColumns);
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();
        return writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
    }

    private static MosaicFileFormat createFormat() {
        return createFormat("");
    }

    private static MosaicFileFormat createFormat(String statsColumns) {
        Options options = new Options();
        if (!statsColumns.isEmpty()) {
            options.set(MosaicFileFormat.STATS_COLUMNS, statsColumns);
        }
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
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
