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

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The {@code vortex.write.edition} option selects the edition that writers use. */
class VortexWriteEditionTest {

    private static final RowType ROW_TYPE =
            RowType.builder().field("id", DataTypes.INT()).field("str", DataTypes.STRING()).build();

    private static final int ROWS = 10_000;

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = LocalFileIO.create();

    @Test
    void testDefaultIsTheEditionThatOldReadersRead() {
        assertThat(VortexFileFormat.WRITE_EDITION.defaultValue()).isEqualTo("core2025.10.0");
    }

    @Test
    void testOptionSelectsTheEdition() throws Exception {
        Path oldEdition = write("old.vortex", new Options());

        Options latest = new Options();
        latest.set(VortexFileFormat.WRITE_EDITION, "core2026.08.3");
        Path newEdition = write("new.vortex", latest);

        // The editions write different layouts, so the files differ, and both are readable.
        assertThat(Files.readAllBytes(tempDir.resolve("old.vortex")))
                .isNotEqualTo(Files.readAllBytes(tempDir.resolve("new.vortex")));
        assertThat(readAll(oldEdition)).isEqualTo(ROWS);
        assertThat(readAll(newEdition)).isEqualTo(ROWS);
    }

    @Test
    void testUnknownEditionIsRejected() {
        Options options = new Options();
        options.set(VortexFileFormat.WRITE_EDITION, "core1999.01.0");

        assertThatThrownBy(() -> write("unknown.vortex", options))
                .hasStackTraceContaining("Unknown Vortex edition: core1999.01.0");
    }

    private Path write(String name, Options options) throws Exception {
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));
        Path file = new Path(new Path(tempDir.toUri()), name);

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < ROWS; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("value-" + i)));
            }
        }
        return file;
    }

    private long readAll(Path file) throws Exception {
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        long next = 0;
        try (FileRecordReader<InternalRow> reader =
                format.createReaderFactory(ROW_TYPE, ROW_TYPE, null)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    assertThat(row.getInt(0)).isEqualTo(next);
                    assertThat(row.getString(1).toString()).isEqualTo("value-" + next);
                    next++;
                }
                batch.releaseBatch();
            }
        }
        return next;
    }
}
