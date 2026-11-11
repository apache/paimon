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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * REQUESTED_COLUMNS_KEY joins the projected column names with the NUL separator, so a name
 * containing that separator (a legal parquet column name) cannot round-trip: it would be split back
 * into different columns, silently rewriting the requested set. Such projections must keep the
 * selective parser off and fall back to the official full parse, which reads the column exactly
 * like the option being disabled.
 */
class ParquetReaderFactoryNulColumnNameTest {

    @TempDir java.nio.file.Path tempDir;

    /** Column name containing the NUL separator character. */
    private static final String NUL_NAME = "a\u0000b";

    /**
     * Projecting the NUL-named column with the selective parser enabled: the name must not be split
     * into "a" and "b" — the read falls back and returns the column's rows.
     */
    @Test
    void testProjectedNameContainingSeparatorFallsBack() throws IOException {
        Path file = writeFile();

        Options conf = new Options();
        conf.set(CoreOptions.PARQUET_FOOTER_SELECTIVE_PARSER_ENABLED, true);
        RowType readType =
                new RowType(
                        Collections.singletonList(new DataField(2, NUL_NAME, DataTypes.BIGINT())));
        ParquetReaderFactory factory = new ParquetReaderFactory(conf, readType, 1024, null);
        try (FileRecordReader<InternalRow> reader = factory.createReader(context(file))) {
            assertThat(readAll(reader)).containsExactly(21L, 22L, 23L);
        }
    }

    /**
     * Ordinary projections over the same file keep working with the selective parser enabled (only
     * the NUL-named chunk is dropped); the guard must not disable them.
     */
    @Test
    void testOrdinaryProjectionUnaffected() throws IOException {
        Path file = writeFile();

        Options conf = new Options();
        conf.set(CoreOptions.PARQUET_FOOTER_SELECTIVE_PARSER_ENABLED, true);
        RowType readType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.BIGINT()),
                                new DataField(1, "b", DataTypes.BIGINT())));
        ParquetReaderFactory factory = new ParquetReaderFactory(conf, readType, 1024, null);
        try (FileRecordReader<InternalRow> reader = factory.createReader(context(file))) {
            List<long[]> values = new ArrayList<>();
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    values.add(new long[] {row.getLong(0), row.getLong(1)});
                }
                batch.releaseBatch();
            }
            assertThat(values)
                    .containsExactly(
                            new long[] {1L, 11L}, new long[] {2L, 12L}, new long[] {3L, 13L});
        }
    }

    private FormatReaderContext context(Path file) throws IOException {
        LocalFileIO fileIO = LocalFileIO.create();
        return new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), null, null);
    }

    /** A valid parquet file with BIGINT columns a, b and a name containing the separator. */
    private Path writeFile() throws IOException {
        RowType fileType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.BIGINT()),
                                new DataField(1, "b", DataTypes.BIGINT()),
                                new DataField(2, NUL_NAME, DataTypes.BIGINT())));
        Path file = new Path(tempDir.toUri().toString(), "nul_name.parquet");
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(fileType).create(out, "zstd");
            writer.addElement(GenericRow.of(1L, 11L, 21L));
            writer.addElement(GenericRow.of(2L, 12L, 22L));
            writer.addElement(GenericRow.of(3L, 13L, 23L));
            writer.close();
        }
        return file;
    }

    private List<Long> readAll(FileRecordReader<InternalRow> reader) throws IOException {
        List<Long> values = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                values.add(row.getLong(0));
            }
            batch.releaseBatch();
        }
        return values;
    }
}
