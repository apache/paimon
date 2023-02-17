/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FormatWriter;
import org.apache.flink.table.store.format.FormatWriterFactory;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.PositionOutputStream;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
        FormatWriter writer = avro.createWriterFactory(rowType).create(out);
        for (InternalRow row : expected) {
            writer.addElement(row);
        }
        writer.finish();
        out.close();

        // read
        RecordReader<InternalRow> reader =
                avro.createReaderFactory(rowType).createReader(LocalFileIO.create(), path);
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
        Assertions.assertThrows(
                RuntimeException.class,
                () -> writerFactory.create(LocalFileIO.create().newOutputStream(path, false)),
                "Unrecognized codec: _unsupported");
    }

    public FileFormat createFileFormat(String codec) {
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.FILE_FORMAT, "avro");
        tableOptions.setString("avro.codec", codec);
        return FileFormat.fromTableOptions(tableOptions, CoreOptions.FILE_FORMAT);
    }
}
