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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.AbstractDictionaryReaderWriterTest;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** IT Case for the read/writer of parquet. */
public class ParquetDictionaryReaderWriterTest extends AbstractDictionaryReaderWriterTest {
    private MockParquetFormat fileFormat;

    public ParquetDictionaryReaderWriterTest() {
        Options options = new Options();
        options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
        // a22 enable dictionary
        options.set("fields.a22.dictionary-enable", "true");
        CoreOptions coreOptions = new CoreOptions(options);
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(
                        options.removePrefix("parquet."), coreOptions.getDictionaryOptions(), 1024);
        MockParquetFormat mockParquetFormat = new MockParquetFormat(formatContext);
        fileFormat = mockParquetFormat;
    }

    @Override
    protected FileFormat getFileFormat() {
        return fileFormat;
    }

    @Test
    public void testWriterFormatOpt() {
        ArrayList<String> expected =
                Lists.newArrayList(
                        "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11",
                        "a12", "a13", "a14", "a15", "a16", "a17", "a18.b1", "a19", "a20", "a21");
        Assertions.assertThat(fileFormat.disableDictionaryPaths).hasSameElementsAs(expected);
    }

    @Test
    public void testFieldPath() {
        List<String> allFieldPath = RowtypeToFieldPathConverter.getAllFieldPath(getRowType());
        ArrayList<String> expected =
                Lists.newArrayList(
                        "a0",
                        "a1",
                        "a2",
                        "a3",
                        "a4",
                        "a5",
                        "a6.list.element",
                        "a7",
                        "a8",
                        "a9",
                        "a10",
                        "a11",
                        "a12",
                        "a13",
                        "a14",
                        "a15",
                        "a16",
                        "a17.key_value.key",
                        "a17.key_value.value",
                        "a19",
                        "a20",
                        "a21.key_value.key",
                        "a22",
                        "a18.b1",
                        "a23.b2.b3.key_value.key",
                        "a23.b2.b3.key_value.value",
                        "a23.b2.b4.list.element",
                        "a23.b2.b5.key_value.key");
        Assertions.assertThat(allFieldPath).hasSameElementsAs(expected);
    }

    @Test
    public void test() throws IOException {
        FormatWriterFactory writerFactory = fileFormat.createWriterFactory(rowType);
        Assertions.assertThat(writerFactory).isInstanceOf(ParquetWriterFactory.class);

        ParquetReaderFactory readerFactory =
                (ParquetReaderFactory) fileFormat.createReaderFactory(rowType);
        RecordReader<InternalRow> reader = readerFactory.createReader(LocalFileIO.create(), path);
        reader.readBatch();
        System.out.println(reader);
    }

    @Test
    public void testTraversalRowType() {
        RowType rowType1 =
                RowType.builder()
                        .field(
                                "a",
                                RowType.builder()
                                        .field(
                                                "b",
                                                RowType.builder()
                                                        .field("c", DataTypes.STRING())
                                                        .field(
                                                                "h",
                                                                DataTypes.ARRAY(DataTypes.STRING()))
                                                        .build())
                                        .build())
                        .field(
                                "d",
                                RowType.builder()
                                        .field("e", DataTypes.STRING())
                                        .field("f", DataTypes.STRING())
                                        .field(
                                                "g",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.STRING()))
                                        .build())
                        .build();

        {
            List<Pair<String, DataType>> expected =
                    Lists.newArrayList(
                            Pair.of("x.d.e", DataTypes.STRING()),
                            Pair.of("x.d.f", DataTypes.STRING()),
                            Pair.of("x.a.b.c", DataTypes.STRING()),
                            Pair.of("x.d.g", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                            Pair.of("x.a.b.h", DataTypes.ARRAY(DataTypes.STRING())));
            Assertions.assertThat(RowtypeToFieldPathConverter.traversalRowType("x", rowType1))
                    .hasSameElementsAs(expected);
        }

        {
            List<String> allFieldPath = RowtypeToFieldPathConverter.getAllFieldPath(rowType1);
            List<String> expected =
                    Lists.newArrayList(
                            "a.b.c",
                            "a.b.h.list.element",
                            "d.e",
                            "d.f",
                            "d.g.key_value.key",
                            "d.g.key_value.value");
            Assertions.assertThat(allFieldPath).hasSameElementsAs(expected);
        }
    }

    class MockParquetFormat extends ParquetFileFormat {
        private List<String> disableDictionaryPaths;

        public MockParquetFormat(FileFormatFactory.FormatContext formatContext) {
            super(formatContext);
        }
    }
}
