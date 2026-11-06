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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round trips of strings and binaries, which Vortex exports as Arrow view types. It covers values
 * stored inline in the view and in data buffers, empty values, and columns with and without nulls.
 */
class VortexStringRoundTripTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("nullable_str", DataTypes.STRING())
                    .field("str", DataTypes.STRING().notNull())
                    .field("bin", DataTypes.BYTES())
                    .build();

    private static final int ROWS = 50_000;

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testValuesAroundTheInlineLimitRoundTrip() throws Exception {
        Path file = new Path(new Path(tempDir.toUri()), "strings-" + System.nanoTime());
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        FileIO fileIO = LocalFileIO.create();

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < ROWS; i++) {
                writer.addElement(row(i));
            }
        }

        int count = 0;
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(ROW_TYPE, ROW_TYPE, null)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    int i = row.getInt(0);
                    assertThat(i).isEqualTo(count);
                    if (i % 7 == 0) {
                        assertThat(row.isNullAt(1)).isTrue();
                        assertThat(row.isNullAt(3)).isTrue();
                    } else {
                        assertThat(row.getString(1).toString()).isEqualTo(value(i));
                        assertThat(row.getBinary(3)).isEqualTo(bytes(i));
                    }
                    assertThat(row.getString(2).toString()).isEqualTo(value(i + 1));
                    count++;
                }
                batch.releaseBatch();
            }
        }
        assertThat(count).isEqualTo(ROWS);
    }

    private static GenericRow row(int i) {
        boolean isNull = i % 7 == 0;
        return GenericRow.of(
                i,
                isNull ? null : BinaryString.fromString(value(i)),
                BinaryString.fromString(value(i + 1)),
                isNull ? null : bytes(i));
    }

    /** Lengths cycle through 0 to 40, crossing the 12-byte inline limit of Arrow views. */
    private static String value(int i) {
        char[] chars = new char[i % 41];
        Arrays.fill(chars, (char) ('a' + i % 26));
        return new String(chars);
    }

    private static byte[] bytes(int i) {
        byte[] bytes = new byte[(i * 3) % 29];
        for (int j = 0; j < bytes.length; j++) {
            bytes[j] = (byte) (i + j);
        }
        return bytes;
    }
}
