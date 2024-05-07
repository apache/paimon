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

package org.apache.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.OrcFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FileFormat}. */
public class FileFormatTest {

    @Test
    public void testWriteRead(@TempDir java.nio.file.Path tempDir) throws IOException {
        FileFormat avro = createFileFormat("snappy");
        RowType rowType = RowType.of(new IntType(), new IntType());

        Path path = new Path(tempDir.toUri().toString(), "1.avro");
        // write

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, 11));
        expected.add(GenericRow.of(2, 22));
        expected.add(GenericRow.of(3, 33));
        PositionOutputStream out = LocalFileIO.create().newOutputStream(path, false);
        FormatWriter writer =
                avro.createWriterFactory(rowType)
                        .create(out, CoreOptions.FILE_COMPRESSION.defaultValue());
        for (InternalRow row : expected) {
            writer.addElement(row);
        }
        writer.finish();
        out.close();

        // read
        RecordReader<InternalRow> reader =
                avro.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        LocalFileIO.create(),
                                        path,
                                        LocalFileIO.create().getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                rowData -> result.add(GenericRow.of(rowData.getInt(0), rowData.getInt(1))));

        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testUnsupportedOption(@TempDir java.nio.file.Path tempDir) {
        FormatWriterFactory writerFactory =
                createFileFormat("_unsupported").createWriterFactory(RowType.of(new IntType()));
        Path path = new Path(tempDir.toUri().toString(), "1.avro");
        assertThatThrownBy(
                        () ->
                                writerFactory.create(
                                        LocalFileIO.create().newOutputStream(path, false),
                                        CoreOptions.FILE_COMPRESSION.defaultValue()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unrecognized codec: _unsupported");
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "Orc", "ORC"})
    public void testCreateFileFormat(String identifier) {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.FILE_FORMAT, identifier);
        tableOptions.set(CoreOptions.READ_BATCH_SIZE, 1024);
        tableOptions.setString(identifier + ".hello", "world");
        FileFormat fileFormat = CoreOptions.createFileFormat(tableOptions, CoreOptions.FILE_FORMAT);
        assertThat(fileFormat instanceof OrcFileFormat).isTrue();

        OrcFileFormat orcFileFormat = (OrcFileFormat) fileFormat;
        assertThat(orcFileFormat.formatContext().formatOptions().get("hello")).isEqualTo("world");
        assertThat(orcFileFormat.formatContext().readBatchSize()).isEqualTo(1024);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "Orc", "ORC"})
    public void testFileFormatOption(String identifier) {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.FILE_FORMAT, identifier);
        tableOptions.set(CoreOptions.READ_BATCH_SIZE, 1024);
        tableOptions.setString(identifier + ".hello", "world");
        FileFormatDiscover fileFormatDiscover =
                FileFormatDiscover.of(new CoreOptions(tableOptions));
        FileFormat fileFormat = fileFormatDiscover.discover(identifier);
        assertThat(fileFormat instanceof OrcFileFormat).isTrue();
        OrcFileFormat orcFileFormat = (OrcFileFormat) fileFormat;
        assertThat(orcFileFormat.formatContext().formatOptions().get("hello")).isEqualTo("world");
        assertThat(orcFileFormat.formatContext().readBatchSize()).isEqualTo(1024);
    }

    public FileFormat createFileFormat(String codec) {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        tableOptions.setString("avro.codec", codec);
        return CoreOptions.createFileFormat(tableOptions, CoreOptions.FILE_FORMAT);
    }
}
