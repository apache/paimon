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
import org.apache.paimon.format.parquet.reader.AbstractColumnReader;
import org.apache.paimon.format.parquet.reader.ArrayColumnReader;
import org.apache.paimon.format.parquet.reader.ColumnReader;
import org.apache.paimon.format.parquet.reader.MapColumnReader;
import org.apache.paimon.format.parquet.reader.RowColumnReader;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/** IT Case for the read/writer of parquet. */
public class ParquetDictionaryReaderWriterTest extends AbstractDictionaryReaderWriterTest {
    private ParquetFileFormat fileFormat;

    public ParquetDictionaryReaderWriterTest() {
        Options options = new Options();
        options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
        // a22 enable dictionary
        options.set("fields.a22.dictionary-enable", "true");
        CoreOptions coreOptions = new CoreOptions(options);
        FileFormatFactory.FormatContext formatContext =
                new FileFormatFactory.FormatContext(
                        options.removePrefix("parquet."), coreOptions.getDictionaryOptions(), 1024);
        fileFormat = new ParquetFileFormat(formatContext);
    }

    @Override
    protected FileFormat getFileFormat() {
        return fileFormat;
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
    public void testFormatDictionaryReader()
            throws IOException, ClassNotFoundException, NoSuchFieldException,
                    IllegalAccessException {
        RowType rowType = getRowType();
        FormatWriterFactory writerFactory = fileFormat.createWriterFactory(rowType);
        Assertions.assertThat(writerFactory).isInstanceOf(ParquetWriterFactory.class);

        ParquetReaderFactory readerFactory =
                (ParquetReaderFactory) fileFormat.createReaderFactory(rowType);
        RecordReader<InternalRow> reader = readerFactory.createReader(LocalFileIO.create(), path);
        reader.readBatch();
        ColumnReader[] parquetReaders = getParquetReaders(reader);

        List<DataField> dataFields = rowType.getFields();
        for (int i = 0; i < dataFields.size(); i++) {
            System.out.println(i);
            DataField dataField = dataFields.get(i);
            if ("a22".equals(dataField.name())) {
                // the field of a22 is enable dictionary
                AbstractColumnReader parquetReader = (AbstractColumnReader) parquetReaders[i];
                Assertions.assertThat(parquetReader.isCurrentPageDictionaryEncoded()).isTrue();
            } else {
                ColumnReader parquetReader = parquetReaders[i];
                assertDisableDictionary(parquetReader, dataField.type());
            }
        }
    }

    public void assertDisableDictionary(ColumnReader reader, DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                AbstractColumnReader abstractColumnReader = (AbstractColumnReader) reader;
                Assertions.assertThat(abstractColumnReader.isCurrentPageDictionaryEncoded())
                        .isFalse();
                break;
            case ARRAY:
                checkArrayDisableDictionary((ArrayColumnReader) reader);
                break;
            case MAP:
                checkMapDisableDictionary((MapColumnReader) reader);
                break;
            case MULTISET:
                checkMultiSetDisableDictionary((MapColumnReader) reader);
                break;
            case ROW:
                RowType rowType = (RowType) dataType;
                RowColumnReader rowColumnReader = (RowColumnReader) reader;

                List<ColumnReader> fieldReaders = rowColumnReader.getFieldReaders();
                List<DataType> fieldTypes = rowType.getFieldTypes();
                for (int i = 0; i < fieldReaders.size(); i++) {
                    ColumnReader columnReader = fieldReaders.get(i);
                    DataType dataField = fieldTypes.get(i);
                    assertDisableDictionary(columnReader, dataField);
                }
                break;
            default:
                throw new RuntimeException("UnSupport Type:" + dataType.getTypeRoot());
        }
    }

    private static void checkMapDisableDictionary(MapColumnReader reader) {
        ArrayColumnReader keyReader = reader.getKeyReader();
        checkArrayDisableDictionary(keyReader);

        ArrayColumnReader valueReader = reader.getValueReader();
        checkArrayDisableDictionary(valueReader);
    }

    private static void checkMultiSetDisableDictionary(MapColumnReader reader) {
        ArrayColumnReader keyReader = reader.getKeyReader();
        checkArrayDisableDictionary(keyReader);
    }

    private static void checkArrayDisableDictionary(ArrayColumnReader reader) {
        ArrayColumnReader parquetReader = reader;
        Assertions.assertThat(parquetReader.isCurrentPageDictionaryEncoded()).isFalse();
    }

    private static ColumnReader[] getParquetReaders(RecordReader<InternalRow> reader)
            throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        Class<?> aClass =
                Class.forName(
                        "org.apache.paimon.format.parquet.ParquetReaderFactory$ParquetReader");
        Field field = aClass.getDeclaredField("columnReaders");
        field.setAccessible(true);
        return (ColumnReader[]) field.get(reader);
    }

    @Test
    public void testTraversalRowType() {
        RowType rowType =
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
            Assertions.assertThat(RowtypeToFieldPathConverter.traversalRowType("x", rowType))
                    .hasSameElementsAs(expected);
        }

        {
            List<String> allFieldPath = RowtypeToFieldPathConverter.getAllFieldPath(rowType);
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
}
