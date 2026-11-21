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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that verify extracting column stats from in-memory ParquetMetadata produces the same
 * results as extracting from the file. This is critical for object stores (like OSS/S3) where the
 * file may not be immediately visible after close.
 */
public class ParquetInMemoryStatsTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    @Test
    public void testImportedPyArrowFloatStats() throws Exception {
        // PyArrow wrote [5, NaN, null] with footer bounds [5, 5].
        java.nio.file.Path local = tempDir.resolve("external.parquet");
        try (java.io.InputStream in =
                getClass().getResourceAsStream("/interop/pyarrow-floating.parquet")) {
            java.nio.file.Files.copy(in, local);
        }
        RowType type = RowType.builder().field("f", new DoubleType()).build();
        Path path = new Path(local.toUri());
        SimpleColStats[] stats =
                FileFormat.fromIdentifier("parquet", new Options())
                        .createStatsExtractor(
                                type, SimpleColStatsCollector.createFullStatsFactories(1))
                        .get()
                        .extract(fileIO, path, fileIO.getFileSize(path));
        assertThat(stats).containsExactly(new SimpleColStats(null, null, 1L));

        RowType narrower = RowType.builder().field("f", new FloatType()).build();
        assertThat(
                        new ParquetSimpleStatsExtractor(
                                        new Options(),
                                        narrower,
                                        SimpleColStatsCollector.createFullStatsFactories(1))
                                .extract(fileIO, path, fileIO.getFileSize(path)))
                .containsExactly(stats[0]);
    }

    @Test
    public void testImportedFloatNullCountsAcrossRowGroups() throws Exception {
        MessageType physical =
                MessageTypeParser.parseMessageType("message root { optional float f; }");
        RowType target = RowType.builder().field("f", new DoubleType()).build();
        SimpleStatsExtractor extractor =
                new ParquetSimpleStatsExtractor(
                        new Options(), target, SimpleColStatsCollector.createFullStatsFactories(1));
        Configuration conf = new Configuration();
        for (boolean complete : new boolean[] {true, false}) {
            Path dest = new Path(tempDir.resolve("combined-" + complete).toUri());
            try (ParquetFileWriter combined =
                    new ParquetFileWriter(
                            HadoopOutputFile.fromPath(
                                    new org.apache.hadoop.fs.Path(dest.toUri()), conf),
                            physical,
                            ParquetFileWriter.Mode.CREATE,
                            1024,
                            0)) {
                combined.start();
                for (int i = 0; i < 2; i++) {
                    org.apache.hadoop.fs.Path part =
                            new org.apache.hadoop.fs.Path(
                                    tempDir.resolve("part-" + complete + "-" + i).toUri());
                    SimpleGroupFactory groups = new SimpleGroupFactory(physical);
                    try (ParquetWriter<Group> writer =
                            ExampleParquetWriter.builder(HadoopOutputFile.fromPath(part, conf))
                                    .withType(physical)
                                    .withConf(conf)
                                    .withStatisticsEnabled(complete || i == 0)
                                    .build()) {
                        writer.write(groups.newGroup());
                        writer.write(groups.newGroup().append("f", Float.NaN));
                    }
                    Path source = new Path(part.toUri());
                    try (ParquetFileReader reader =
                            ParquetUtil.getParquetReader(
                                    fileIO, source, fileIO.getFileSize(source), new Options())) {
                        reader.appendTo(combined);
                    }
                }
                combined.end(Collections.emptyMap());
            }
            assertThat(extractor.extract(fileIO, dest, fileIO.getFileSize(dest)))
                    .containsExactly(new SimpleColStats(null, null, complete ? 2L : null));
        }
    }

    @Test
    public void testRejectIncompatibleFloatImports() throws Exception {
        RowType target = RowType.builder().field("f", new FloatType()).build();
        SimpleStatsExtractor extractor =
                new ParquetSimpleStatsExtractor(
                        new Options(), target, SimpleColStatsCollector.createFullStatsFactories(1));
        for (String field : new String[] {"optional binary f", "repeated float f"}) {
            MessageType physical =
                    MessageTypeParser.parseMessageType("message root { " + field + "; }");
            Path path = new Path(tempDir.resolve(field.replace(' ', '-')).toUri());
            Configuration conf = new Configuration();
            try (ParquetWriter<Group> writer =
                    ExampleParquetWriter.builder(
                                    HadoopOutputFile.fromPath(
                                            new org.apache.hadoop.fs.Path(path.toUri()), conf))
                            .withType(physical)
                            .withConf(conf)
                            .build()) {
                writer.write(new SimpleGroupFactory(physical).newGroup());
            }
            assertThatThrownBy(() -> extractor.extract(fileIO, path, fileIO.getFileSize(path)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("scalar Parquet FLOAT or DOUBLE");
        }
    }

    @Test
    public void testInMemoryStatsMatchFileStats() throws Exception {
        RowType rowType =
                RowType.builder()
                        .fields(
                                new VarCharType(100),
                                new BooleanType(),
                                new TinyIntType(),
                                new SmallIntType(),
                                new IntType(),
                                new BigIntType(),
                                new FloatType(),
                                new DoubleType(),
                                new DecimalType(10, 2),
                                new DateType())
                        .build();

        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        Path path = new Path(tempDir.toString() + "/test_inmemory_stats.parquet");

        // Write test data
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "SNAPPY");

        for (int i = 0; i < 100; i++) {
            GenericRow row = new GenericRow(10);
            row.setField(0, BinaryString.fromString("value_" + i));
            row.setField(1, i % 2 == 0);
            row.setField(2, (byte) (i % 127));
            row.setField(3, (short) (i * 10));
            row.setField(4, i * 100);
            row.setField(5, (long) i * 1000);
            // Start from 1 to avoid -0.0 vs 0.0 IEEE 754 representation difference
            // between in-memory and serialized Parquet statistics
            row.setField(6, (i + 1) * 1.1f);
            row.setField(7, (i + 1) * 2.2);
            row.setField(8, Decimal.fromBigDecimal(new BigDecimal(i + ".99"), 10, 2));
            row.setField(9, 18000 + i); // date as days since epoch
            writer.addElement(row);
        }

        // Also add some nulls
        for (int i = 0; i < 10; i++) {
            GenericRow row = new GenericRow(10);
            // Leave all fields null
            writer.addElement(row);
        }

        writer.close();
        out.close();

        // Get in-memory metadata from writer
        Object writerMetadata = writer.writerMetadata();
        assertThat(writerMetadata).isNotNull();
        assertThat(writerMetadata).isInstanceOf(ParquetMetadata.class);

        // Create stats extractors
        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] statsCollectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(p -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);
        SimpleStatsExtractor extractor =
                format.createStatsExtractor(rowType, statsCollectors).get();

        // Extract stats from file (original path)
        long fileSize = fileIO.getFileSize(path);
        SimpleColStats[] fromFile = extractor.extract(fileIO, path, fileSize);

        // Extract stats from in-memory metadata (new path)
        SimpleColStats[] fromMemory = extractor.extract(fileIO, path, fileSize, writerMetadata);

        for (int i = 0; i < fromFile.length; i++) {
            if (i == 6 || i == 7) {
                assertThat(fromFile[i])
                        .isEqualTo(new SimpleColStats(null, null, fromMemory[i].nullCount()));
                assertThat(fromMemory[i].min()).isNotNull();
                assertThat(fromMemory[i].max()).isNotNull();
            } else {
                assertThat(fromFile[i]).isEqualTo(fromMemory[i]);
            }
        }

        // Also verify the stats are correct
        assertThat(fromFile[0].min()).isEqualTo(BinaryString.fromString("value_0"));
        assertThat(fromFile[0].max()).isEqualTo(BinaryString.fromString("value_99"));
        assertThat(fromFile[0].nullCount()).isEqualTo(10L);

        assertThat(fromFile[4].min()).isEqualTo(0);
        assertThat(fromFile[4].max()).isEqualTo(9900);
        assertThat(fromFile[4].nullCount()).isEqualTo(10L);
    }

    @Test
    public void testInMemoryStatsFallbackWhenMetadataIsNull() throws Exception {
        RowType rowType = RowType.builder().fields(new IntType(), new VarCharType(50)).build();

        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        Path path = new Path(tempDir.toString() + "/test_fallback.parquet");

        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = writerFactory.create(out, "SNAPPY");

        for (int i = 0; i < 10; i++) {
            GenericRow row = new GenericRow(2);
            row.setField(0, i);
            row.setField(1, BinaryString.fromString("text_" + i));
            writer.addElement(row);
        }
        writer.close();
        out.close();

        int fieldCount = rowType.getFieldCount();
        SimpleColStatsCollector.Factory[] statsCollectors =
                IntStream.range(0, fieldCount)
                        .mapToObj(p -> SimpleColStatsCollector.from("full"))
                        .toArray(SimpleColStatsCollector.Factory[]::new);
        SimpleStatsExtractor extractor =
                format.createStatsExtractor(rowType, statsCollectors).get();

        long fileSize = fileIO.getFileSize(path);

        // Extract with null metadata (should fall back to file reading)
        SimpleColStats[] fromFallback = extractor.extract(fileIO, path, fileSize, null);

        // Extract from file directly
        SimpleColStats[] fromFile = extractor.extract(fileIO, path, fileSize);

        // Results should be identical
        assertThat(fromFallback).isEqualTo(fromFile);
    }

    @Test
    public void testWriterMetadataDefaultIsNull() throws Exception {
        // A non-Parquet FormatWriter (or any FormatWriter that doesn't override writerMetadata())
        // should return null by default
        FormatWriter mockWriter =
                new FormatWriter() {
                    @Override
                    public void addElement(org.apache.paimon.data.InternalRow element) {}

                    @Override
                    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
                        return false;
                    }

                    @Override
                    public void close() {}
                };

        assertThat(mockWriter.writerMetadata()).isNull();
    }
}
