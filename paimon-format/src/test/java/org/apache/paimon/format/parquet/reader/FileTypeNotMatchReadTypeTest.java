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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.parquet.ParquetReaderFactory;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
import org.apache.paimon.format.parquet.writer.ParquetRowDataBuilder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_KEY_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_VALUE_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.PAIMON_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

/** Test field type not match correctly with read type. */
public class FileTypeNotMatchReadTypeTest {

    private static final Random RANDOM = new Random();
    @TempDir private Path tempDir;

    @Test
    public void testTimestamp() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;
        for (int i = 0; i < 100; i++) {
            int writePrecision = RANDOM.nextInt(10);
            int readPrecision = writePrecision == 0 ? 0 : RANDOM.nextInt(writePrecision);

            // precision 0-3 and 3-6 are all long type (INT64) in file.
            // but precision 0-3 is TIMESTAMP_MICROS and 3-6 is TIMESTAMP_MILLIS in file.
            // so we need to set readPrecision to 4 if writePrecision is 4-6.
            if (readPrecision <= 3 && writePrecision > 3) {
                readPrecision = 4;
            }

            RowType rowTypeWrite = RowType.of(DataTypes.TIMESTAMP(writePrecision));
            RowType rowTypeRead = RowType.of(DataTypes.TIMESTAMP(readPrecision));

            ParquetRowDataBuilder parquetRowDataBuilder =
                    new ParquetRowDataBuilder(
                            new LocalOutputFile(new File(fileWholePath).toPath()),
                            rowTypeWrite,
                            null);

            ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();
            Timestamp timestamp = Timestamp.now();
            parquetWriter.write(GenericRow.of(timestamp));
            parquetWriter.write(GenericRow.of(Timestamp.now()));
            parquetWriter.close();

            ParquetReaderFactory parquetReaderFactory =
                    new ParquetReaderFactory(new Options(), rowTypeRead, 100, null);

            File file = new File(fileWholePath);
            FileRecordReader<InternalRow> fileRecordReader =
                    parquetReaderFactory.createReader(
                            new FormatReaderContext(
                                    LocalFileIO.create(),
                                    new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                    file.length()));

            InternalRow row = fileRecordReader.readBatch().next();
            Timestamp getTimestamp = row.getTimestamp(0, readPrecision);
            assertThat(timestamp.getMillisecond()).isEqualTo(getTimestamp.getMillisecond());
            file.delete();
        }
    }

    @Test
    public void testDecimal() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;
        for (int i = 0; i < 100; i++) {
            int writePrecision = 1 + RANDOM.nextInt(30);
            int readPrecision = 1 + RANDOM.nextInt(writePrecision);

            RowType rowTypeWrite = RowType.of(DataTypes.DECIMAL(writePrecision, 0));
            RowType rowTypeRead = RowType.of(DataTypes.DECIMAL(readPrecision, 0));

            ParquetRowDataBuilder parquetRowDataBuilder =
                    new ParquetRowDataBuilder(
                            new LocalOutputFile(new File(fileWholePath).toPath()),
                            rowTypeWrite,
                            null);

            ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();
            Decimal decimal =
                    Decimal.fromBigDecimal(new java.math.BigDecimal(1.0), writePrecision, 0);
            parquetWriter.write(GenericRow.of(decimal));
            parquetWriter.write(
                    GenericRow.of(
                            Decimal.fromBigDecimal(
                                    new java.math.BigDecimal(2.0), writePrecision, 0)));
            parquetWriter.close();

            ParquetReaderFactory parquetReaderFactory =
                    new ParquetReaderFactory(new Options(), rowTypeRead, 100, null);

            File file = new File(fileWholePath);
            FileRecordReader<InternalRow> fileRecordReader =
                    parquetReaderFactory.createReader(
                            new FormatReaderContext(
                                    LocalFileIO.create(),
                                    new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                    file.length()));

            InternalRow row = fileRecordReader.readBatch().next();
            Decimal getDecimal = row.getDecimal(0, readPrecision, 0);
            assertThat(decimal.toUnscaledLong()).isEqualTo(getDecimal.toUnscaledLong());
            file.delete();
        }
    }

    @Test
    public void testArray() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;

        RowType rowTypeWrite = RowType.of(DataTypes.ARRAY(DataTypes.INT()));
        MessageType messageType = Util.convertToParquetMessageType(rowTypeWrite);
        ParquetRowDataBuilderForTest parquetRowDataBuilder =
                new ParquetRowDataBuilderForTest(
                        new LocalOutputFile(new File(fileWholePath).toPath()),
                        rowTypeWrite,
                        messageType);
        ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();

        GenericArray genericArray = new GenericArray(new int[] {1, 32, 3, 4, 346});
        parquetWriter.write(GenericRow.of(genericArray));
        parquetWriter.close();

        ParquetReaderFactory parquetReaderFactory =
                new ParquetReaderFactory(new Options(), rowTypeWrite, 100, null);

        File file = new File(fileWholePath);
        FileRecordReader<InternalRow> fileRecordReader =
                parquetReaderFactory.createReader(
                        new FormatReaderContext(
                                LocalFileIO.create(),
                                new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                file.length()));

        InternalRow row = fileRecordReader.readBatch().next();
        int i = row.getArray(0).getInt(0);
        assertThat(i).isEqualTo(1);
        file.delete();
    }

    @Test
    public void testArray2() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;

        SimpleGroupWriteSupport simpleGroupWriteSupport = new SimpleGroupWriteSupport();
        simpleGroupWriteSupport.writeTest(
                fileWholePath,
                Arrays.asList(
                        new SimpleGroupWriteSupport.SimpleGroup(Arrays.asList(1, 21, 242)),
                        new SimpleGroupWriteSupport.SimpleGroup(Arrays.asList(4, 221, 12))));

        RowType rowType =
                RowType.of(new DataField(0, "list_of_ints", DataTypes.ARRAY(DataTypes.INT())));

        ParquetReaderFactory parquetReaderFactory =
                new ParquetReaderFactory(new Options(), rowType, 100, null);

        File file = new File(fileWholePath);
        FileRecordReader<InternalRow> fileRecordReader =
                parquetReaderFactory.createReader(
                        new FormatReaderContext(
                                LocalFileIO.create(),
                                new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                file.length()));

        FileRecordIterator<InternalRow> batch = fileRecordReader.readBatch();
        InternalRow row = batch.next();
        assertThat(row.getArray(0).getInt(0)).isEqualTo(1);
        assertThat(row.getArray(0).getInt(1)).isEqualTo(21);
        assertThat(row.getArray(0).getInt(2)).isEqualTo(242);
        row = batch.next();
        assertThat(row.getArray(0).getInt(0)).isEqualTo(4);
        assertThat(row.getArray(0).getInt(1)).isEqualTo(221);
        assertThat(row.getArray(0).getInt(2)).isEqualTo(12);
        file.delete();
    }

    @Test
    public void testMap() throws Exception {
        String fileName = "test.parquet";
        String fileWholePath = tempDir + "/" + fileName;

        RowType rowTypeWrite =
                RowType.of(
                        DataTypes.MAP(
                                DataTypes.STRING(),
                                DataTypes.MAP(DataTypes.INT(), DataTypes.INT())));
        MessageType messageType = Util.convertToParquetMessageType(rowTypeWrite);
        ParquetRowDataBuilderForTest parquetRowDataBuilder =
                new ParquetRowDataBuilderForTest(
                        new LocalOutputFile(new File(fileWholePath).toPath()),
                        rowTypeWrite,
                        messageType);
        ParquetWriter<InternalRow> parquetWriter = parquetRowDataBuilder.build();

        Map<Integer, Integer> mapInner = new HashMap<>();
        mapInner.put(1, 2);
        mapInner.put(2, 3);
        Map<BinaryString, InternalMap> mapOuter = new HashMap<>();
        mapOuter.put(BinaryString.fromString("hello"), new GenericMap(mapInner));

        GenericMap genericMap = new GenericMap(mapOuter);
        parquetWriter.write(GenericRow.of(genericMap));
        parquetWriter.close();

        ParquetReaderFactory parquetReaderFactory =
                new ParquetReaderFactory(new Options(), rowTypeWrite, 100, null);

        File file = new File(fileWholePath);
        FileRecordReader<InternalRow> fileRecordReader =
                parquetReaderFactory.createReader(
                        new FormatReaderContext(
                                LocalFileIO.create(),
                                new org.apache.paimon.fs.Path(tempDir.toString(), fileName),
                                file.length()));

        InternalRow row = fileRecordReader.readBatch().next();
        int i = row.getMap(0).valueArray().getMap(0).keyArray().getInt(0);
        int j = row.getMap(0).valueArray().getMap(0).valueArray().getInt(1);
        assertThat(i).isEqualTo(1);
        assertThat(j).isEqualTo(3);
        file.delete();
    }

    /** To build special list and map message type. */
    static class Util {
        public static MessageType convertToParquetMessageType(RowType rowType) {
            return new MessageType(PAIMON_SCHEMA, convertToParquetTypes(rowType));
        }

        public static Type[] convertToParquetTypes(RowType rowType) {
            return rowType.getFields().stream()
                    .map(Util::convertToParquetType)
                    .toArray(Type[]::new);
        }

        public static Type convertToParquetType(DataField field) {
            return convertToParquetType(field.name(), field.type(), field.id(), 0);
        }

        public static Type convertToParquetType(
                String name, DataType type, int fieldId, int depth) {
            Type.Repetition repetition =
                    type.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
            switch (type.getTypeRoot()) {
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
                case ROW:
                case VARIANT:
                case MULTISET:
                    return ParquetSchemaConverter.convertToParquetType(name, type, fieldId, depth);
                case ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    Type elementParquetType =
                            convertToParquetType(
                                            "array_element",
                                            arrayType.getElementType(),
                                            fieldId,
                                            depth + 1)
                                    .withId(
                                            SpecialFields.getArrayElementFieldId(
                                                    fieldId, depth + 1));
                    Type groupMiddle =
                            new GroupType(Type.Repetition.REPEATED, "bag", elementParquetType);
                    return new GroupType(repetition, name, OriginalType.LIST, groupMiddle);
                case MAP:
                    MapType mapType = (MapType) type;
                    DataType keyType = mapType.getKeyType();
                    if (keyType.isNullable()) {
                        keyType = keyType.copy(false);
                    }
                    Type mapKeyParquetType =
                            convertToParquetType(MAP_KEY_NAME, keyType, fieldId, depth + 1)
                                    .withId(SpecialFields.getMapKeyFieldId(fieldId, depth + 1));
                    Type mapValueParquetType =
                            convertToParquetType(
                                            MAP_VALUE_NAME,
                                            mapType.getValueType(),
                                            fieldId,
                                            depth + 1)
                                    .withId(SpecialFields.getMapValueFieldId(fieldId, depth + 1));
                    return ConversionPatterns.mapType(
                                    repetition,
                                    name,
                                    "custom_map",
                                    mapKeyParquetType,
                                    mapValueParquetType)
                            .withId(fieldId);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }
    }
}
