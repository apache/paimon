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

package org.apache.flink.table.store.file.format;

import org.apache.flink.table.store.file.append.AppendOnlyCompactManager;
import org.apache.flink.table.store.file.append.AppendOnlyWriter;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.io.DataFilePathFactory;
import org.apache.flink.table.store.file.io.KeyValueFileReadWriteTest;
import org.apache.flink.table.store.file.io.KeyValueFileWriterFactory;
import org.apache.flink.table.store.file.utils.CommitIncrement;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.VarCharType;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.LinkedList;

/** test file format suffix. */
public class FileFormatSuffixTest extends KeyValueFileReadWriteTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new DataType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});

    @Test
    public void testFileSuffix(@TempDir java.nio.file.Path tempDir) throws Exception {
        String format = "avro";
        KeyValueFileWriterFactory writerFactory = createWriterFactory(tempDir.toString(), format);
        Path path = writerFactory.pathFactory().newPath();
        Assertions.assertTrue(path.getPath().endsWith(format));

        DataFilePathFactory dataFilePathFactory =
                new DataFilePathFactory(new Path(tempDir.toString()), "dt=1", 1, format);
        FileFormat fileFormat = FileFormat.fromIdentifier(format, new Options());
        LinkedList<DataFileMeta> toCompact = new LinkedList<>();
        AppendOnlyWriter appendOnlyWriter =
                new AppendOnlyWriter(
                        LocalFileIO.create(),
                        0,
                        fileFormat,
                        10,
                        SCHEMA,
                        0,
                        new AppendOnlyCompactManager(
                                LocalFileIO.create(),
                                null,
                                toCompact,
                                4,
                                10,
                                10,
                                null,
                                dataFilePathFactory), // not used
                        false,
                        dataFilePathFactory,
                        null);
        appendOnlyWriter.write(
                GenericRow.of(1, BinaryString.fromString("aaa"), BinaryString.fromString("1")));
        CommitIncrement increment = appendOnlyWriter.prepareCommit(true);
        appendOnlyWriter.close();

        DataFileMeta meta = increment.newFilesIncrement().newFiles().get(0);
        Assertions.assertTrue(meta.fileName().endsWith(format));
    }
}
