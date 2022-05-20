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

package org.apache.flink.table.store.format;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.data.DataFilePathFactory;
import org.apache.flink.table.store.file.data.DataFileTest;
import org.apache.flink.table.store.file.data.DataFileWriter;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.writer.AppendOnlyWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

/** test file format suffix. */
public class FileFormatSuffixTest extends DataFileTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(), new VarCharType()},
                    new String[] {"id", "name", "dt"});

    @ParameterizedTest
    @ValueSource(strings = {"avro", "parquet", "orc"})
    public void testFileSuffix(String format, @TempDir java.nio.file.Path tempDir)
            throws Exception {
        DataFileWriter dataFileWriter = createDataFileWriter(tempDir.toString(), format);
        Path path = dataFileWriter.pathFactory().newPath();
        Assertions.assertTrue(path.getPath().endsWith(format));

        DataFilePathFactory dataFilePathFactory =
                new DataFilePathFactory(new Path(tempDir.toString()), "dt=1", 1, format);
        FileFormat fileFormat =
                FileFormat.fromIdentifier(
                        Thread.currentThread().getContextClassLoader(),
                        format,
                        new Configuration());
        AppendOnlyWriter appendOnlyWriter =
                new AppendOnlyWriter(fileFormat, 10, SCHEMA, 10, dataFilePathFactory);
        appendOnlyWriter.write(
                ValueKind.ADD,
                BinaryRowDataUtil.EMPTY_ROW,
                GenericRowData.of(1, StringData.fromString("aaa"), StringData.fromString("1")));
        List<DataFileMeta> result = appendOnlyWriter.close();

        DataFileMeta meta = result.get(0);
        Assertions.assertTrue(meta.fileName().endsWith(format));
    }
}
