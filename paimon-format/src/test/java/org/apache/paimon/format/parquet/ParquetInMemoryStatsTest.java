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

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify extracting column stats from in-memory ParquetMetadata produces the same
 * results as extracting from the file. This is critical for object stores (like OSS/S3) where the
 * file may not be immediately visible after close.
 */
public class ParquetInMemoryStatsTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

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

        // They should be exactly the same
        assertThat(fromMemory).isEqualTo(fromFile);

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
