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

package org.apache.paimon.format;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** test bulk based format. */
public class BulkFileFormatTest {

    @Test
    public void testAvro(@TempDir java.nio.file.Path tempDir) throws IOException {
        testFormatWriteRead(tempDir, "avro", "snappy");
    }

    @Test
    public void testOrc(@TempDir java.nio.file.Path tempDir) throws IOException {
        testFormatWriteRead(tempDir, "orc", "snappy");
    }

    public FileFormat createFileFormat(String format, String codec) {
        Options tableOptions = new Options();
        tableOptions.setString(format + ".codec", codec);
        return FileFormat.fromIdentifier(format, tableOptions);
    }

    public void testFormatWriteRead(
            @TempDir java.nio.file.Path tempDir, String format, String codec) throws IOException {
        FileFormat fileFormat = createFileFormat(format, codec);
        RowType rowType =
                RowType.builder().fields(Arrays.asList(new IntType(), new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "1." + format);

        // write
        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, 1));
        expected.add(GenericRow.of(2, 2));
        expected.add(GenericRow.of(3, 3));
        PositionOutputStream out = new LocalFileIO().newOutputStream(path, false);
        FormatWriter writer = fileFormat.createWriterFactory(rowType).create(out, "LZ4");
        for (InternalRow row : expected) {
            writer.addElement(row);
        }
        writer.close();
        out.close();

        // read
        RecordReader<InternalRow> reader =
                fileFormat
                        .createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        new LocalFileIO(),
                                        path,
                                        new LocalFileIO().getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                rowData -> result.add(GenericRow.of(rowData.getInt(0), rowData.getInt(0))));

        assertThat(result).isEqualTo(expected);
    }
}
