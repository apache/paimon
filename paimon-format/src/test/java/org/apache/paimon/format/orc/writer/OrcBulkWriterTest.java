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

package org.apache.paimon.format.orc.writer;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.format.orc.OrcWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.orc.Reader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

class OrcBulkWriterTest {

    @Test
    void testStripeSizeCheckRatio(@TempDir java.nio.file.Path tempDir) throws IOException {
        Options options = new Options();
        options.set(CoreOptions.WRITE_BATCH_SIZE, 128);
        options.set("orc.stripe.size", "65536");
        options.set("orc.rows.between.memory.checks", "5000");
        options.set("orc.stripe.size.check.ratio", "1");
        options.set("orc.column.encoding.direct", "payload");
        FileFormat orc = FileFormat.fromIdentifier("orc", options);

        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT())
                        .field("payload", DataTypes.STRING())
                        .build();
        Path path = new Path(tempDir.toUri().toString(), "large-rows.orc");
        LocalFileIO fileIO = LocalFileIO.create();
        String payload = new String(new char[2048]).replace('\0', 'x');
        int rowCount = 512;

        try (PositionOutputStream out = fileIO.newOutputStream(path, false);
                FormatWriter writer = orc.createWriterFactory(rowType).create(out, "none")) {
            for (int i = 0; i < rowCount; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString(i + "-" + payload)));
            }
        }

        try (Reader reader =
                OrcReaderFactory.createReader(
                        new org.apache.hadoop.conf.Configuration(false), fileIO, path, null)) {
            Assertions.assertThat(reader.getStripes()).hasSizeGreaterThan(1);
            Assertions.assertThat(reader.getNumberOfRows()).isEqualTo(rowCount);
        }

        int[] actualRowCount = {0};
        try (RecordReader<InternalRow> reader =
                orc.createReaderFactory(rowType, rowType, null)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, path, fileIO.getFileSize(path), null, null))) {
            reader.forEachRemaining(
                    row -> {
                        int id = actualRowCount[0]++;
                        Assertions.assertThat(row.getInt(0)).isEqualTo(id);
                        Assertions.assertThat(row.getString(1).toString())
                                .isEqualTo(id + "-" + payload);
                    });
        }
        Assertions.assertThat(actualRowCount[0]).isEqualTo(rowCount);
    }

    @Test
    void testRowBatch(@TempDir java.nio.file.Path tempDir) throws IOException {
        Options options = new Options();
        options.set(CoreOptions.WRITE_BATCH_SIZE, 1);
        options.set(CoreOptions.WRITE_BATCH_MEMORY, MemorySize.parse("1 Kb"));
        FileFormat orc = FileFormat.fromIdentifier("orc", options);
        Assertions.assertThat(orc).isInstanceOf(OrcFileFormat.class);

        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.STRING())
                        .build();
        FormatWriterFactory writerFactory = orc.createWriterFactory(rowType);
        Assertions.assertThat(writerFactory).isInstanceOf(OrcWriterFactory.class);

        Path path = new Path(tempDir.toUri().toString(), "1.orc");
        PositionOutputStream out = LocalFileIO.create().newOutputStream(path, false);
        FormatWriter formatWriter = writerFactory.create(out, "zstd");

        Assertions.assertThat(formatWriter).isInstanceOf(OrcBulkWriter.class);

        OrcBulkWriter orcBulkWriter = (OrcBulkWriter) formatWriter;
        Assertions.assertThat(orcBulkWriter.getRowBatch().getMaxSize()).isEqualTo(1);
        Assertions.assertThat(orcBulkWriter.getMemoryLimit()).isEqualTo(1024);
    }
}
