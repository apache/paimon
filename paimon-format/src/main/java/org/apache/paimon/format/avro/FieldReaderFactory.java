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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** Factory to create {@link FieldReader}. */
public class FieldReaderFactory implements AvroSchemaVisitor<FieldReader> {

    private static final FieldReader STRING_READER = new StringReader();

    private static final FieldReader BYTES_READER = new BytesReader();

    private static final FieldReader BOOLEAN_READER = new BooleanReader();

    private static final FieldReader TINYINT_READER = new TinyIntReader();

    private static final FieldReader SMALLINT_READER = new SmallIntReader();

    private static final FieldReader INT_READER = new IntReader();

    private static final FieldReader BIGINT_READER = new BigIntReader();

    private static final FieldReader FLOAT_READER = new FloatReader();

    private static final FieldReader DOUBLE_READER = new DoubleReader();

    private static final FieldReader TIMESTAMP_MILLS_READER = new TimestampMillsReader();

    private static final FieldReader TIMESTAMP_MICROS_READER = new TimestampMicrosReader();

    @Override
    public FieldReader visitUnion(Schema schema, DataType type) {
        return new NullableReader(visit(schema.getTypes().get(1), type));
    }

    @Override
    public FieldReader visitString() {
        return STRING_READER;
    }

    @Override
    public FieldReader visitBytes() {
        return BYTES_READER;
    }

    @Override
    public FieldReader visitInt() {
        return INT_READER;
    }

    @Override
    public FieldReader visitTinyInt() {
        return TINYINT_READER;
    }

    @Override
    public FieldReader visitSmallInt() {
        return SMALLINT_READER;
    }

    @Override
    public FieldReader visitBoolean() {
        return BOOLEAN_READER;
    }

    @Override
    public FieldReader visitBigInt() {
        return BIGINT_READER;
    }

    @Override
    public FieldReader visitFloat() {
        return FLOAT_READER;
    }

    @Override
    public FieldReader visitDouble() {
        return DOUBLE_READER;
    }

    @Override
    public FieldReader visitTimestampMillis(int precision) {
        return TIMESTAMP_MILLS_READER;
    }

    @Override
    public FieldReader visitTimestampMicros(int precision) {
        return TIMESTAMP_MICROS_READER;
    }

    @Override
    public FieldReader visitDecimal(int precision, int scale) {
        return new DecimalReader(precision, scale);
    }

    @Override
    public FieldReader visitArray(Schema schema, DataType elementType) {
        FieldReader elementReader = visit(schema.getElementType(), elementType);
        return new ArrayReader(elementReader);
    }

    @Override
    public FieldReader visitMap(Schema schema, DataType valueType) {
        FieldReader valueReader = visit(schema.getValueType(), valueType);
        return new MapReader(valueReader);
    }

    @Override
    public FieldReader visitRecord(Schema schema, List<DataType> fieldTypes) {
        return new RowReader(schema, fieldTypes);
    }

    private static class NullableReader implements FieldReader {

        private final FieldReader reader;

        public NullableReader(FieldReader reader) {
            this.reader = reader;
        }

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            int index = decoder.readIndex();
            return index == 0 ? null : reader.read(decoder, reuse);
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            int index = decoder.readIndex();
            if (index == 1) {
                reader.skip(decoder);
            }
        }
    }

    private static class StringReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            Utf8 utf8 = null;
            if (reuse instanceof BinaryString) {
                utf8 = new Utf8(((BinaryString) reuse).toBytes());
            }

            Utf8 string = decoder.readString(utf8);
            return BinaryString.fromBytes(string.getBytes(), 0, string.getByteLength());
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.skipString();
        }
    }

    private static class BytesReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readBytes(null).array();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.skipBytes();
        }
    }

    private static class BooleanReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readBoolean();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readBoolean();
        }
    }

    private static class TinyIntReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return (byte) decoder.readInt();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readInt();
        }
    }

    private static class SmallIntReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return (short) decoder.readInt();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readInt();
        }
    }

    private static class IntReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readInt();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readInt();
        }
    }

    private static class BigIntReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readLong();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readLong();
        }
    }

    private static class FloatReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readFloat();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readFloat();
        }
    }

    private static class DoubleReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return decoder.readDouble();
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readDouble();
        }
    }

    private static class DecimalReader implements FieldReader {

        private final int precision;
        private final int scale;

        private DecimalReader(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            byte[] bytes = (byte[]) BYTES_READER.read(decoder, null);
            return Decimal.fromBigDecimal(
                    new BigDecimal(new BigInteger(bytes), scale), precision, scale);
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            BYTES_READER.skip(decoder);
        }
    }

    private static class TimestampMillsReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return Timestamp.fromEpochMillis(decoder.readLong());
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readLong();
        }
    }

    private static class TimestampMicrosReader implements FieldReader {

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            return Timestamp.fromMicros(decoder.readLong());
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            decoder.readLong();
        }
    }

    private static class ArrayReader implements FieldReader {

        private final FieldReader elementReader;
        private final List<Object> reusedList = new ArrayList<>();

        private ArrayReader(FieldReader elementReader) {
            this.elementReader = elementReader;
        }

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            reusedList.clear();
            long chunkLength = decoder.readArrayStart();

            while (chunkLength > 0) {
                for (int i = 0; i < chunkLength; i += 1) {
                    reusedList.add(elementReader.read(decoder, null));
                }

                chunkLength = decoder.arrayNext();
            }

            return new GenericArray(reusedList.toArray());
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            long chunkLength = decoder.readArrayStart();

            while (chunkLength > 0) {
                for (int i = 0; i < chunkLength; i += 1) {
                    elementReader.skip(decoder);
                }

                chunkLength = decoder.arrayNext();
            }
        }
    }

    private static class MapReader implements FieldReader {

        private final FieldReader valueReader;
        private final List<Object> reusedKeyList = new ArrayList<>();
        private final List<Object> reusedValueList = new ArrayList<>();

        private MapReader(FieldReader valueReader) {
            this.valueReader = valueReader;
        }

        @Override
        public Object read(Decoder decoder, Object reuse) throws IOException {
            reusedKeyList.clear();
            reusedValueList.clear();

            long chunkLength = decoder.readMapStart();

            while (chunkLength > 0) {
                for (int i = 0; i < chunkLength; i += 1) {
                    reusedKeyList.add(STRING_READER.read(decoder, null));
                    reusedValueList.add(valueReader.read(decoder, null));
                }

                chunkLength = decoder.mapNext();
            }

            Map<Object, Object> map = new HashMap<>();
            Object[] keys = reusedKeyList.toArray();
            Object[] values = reusedValueList.toArray();
            for (int i = 0; i < keys.length; i++) {
                map.put(keys[i], values[i]);
            }

            return new GenericMap(map);
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            long chunkLength = decoder.readMapStart();

            while (chunkLength > 0) {
                for (int i = 0; i < chunkLength; i += 1) {
                    STRING_READER.skip(decoder);
                    valueReader.skip(decoder);
                }

                chunkLength = decoder.mapNext();
            }
        }
    }

    public RowReader createRowReader(Schema schema, List<DataType> fieldTypes, int[] projection) {
        return new RowReader(schema, fieldTypes, projection);
    }

    /** A {@link FieldReader} to read {@link InternalRow}. */
    public class RowReader implements FieldReader {

        private final FieldReader[] fieldReaders;
        private final int[] projection;
        private final int[][] mapping;

        public RowReader(Schema schema, List<DataType> fieldTypes) {
            this(schema, fieldTypes, IntStream.range(0, fieldTypes.size()).toArray());
        }

        public RowReader(Schema schema, List<DataType> fieldTypes, int[] projection) {
            List<Schema.Field> schemaFields = schema.getFields();
            this.fieldReaders = new FieldReader[schemaFields.size()];
            for (int i = 0, fieldsSize = schemaFields.size(); i < fieldsSize; i++) {
                Schema.Field field = schemaFields.get(i);
                DataType type = fieldTypes.get(i);
                fieldReaders[i] = visit(field.schema(), type);
            }
            this.projection = projection;

            // use fieldTypes to compatible with less fields in avro

            @SuppressWarnings("unchecked")
            List<Integer>[] mapping = new List[fieldTypes.size()];
            for (int i = 0; i < projection.length; i++) {
                List<Integer> columns = mapping[projection[i]];
                if (columns == null) {
                    columns = new ArrayList<>();
                    mapping[projection[i]] = columns;
                }
                columns.add(i);
            }

            this.mapping = new int[fieldTypes.size()][];
            for (int i = 0; i < mapping.length; i++) {
                List<Integer> fields = mapping[i];
                if (fields != null) {
                    this.mapping[i] = fields.stream().mapToInt(Integer::intValue).toArray();
                }
            }
        }

        @Override
        public InternalRow read(Decoder decoder, Object reuse) throws IOException {
            GenericRow row;
            if (reuse instanceof GenericRow
                    && ((GenericRow) reuse).getFieldCount() == projection.length) {
                row = (GenericRow) reuse;
            } else {
                row = new GenericRow(projection.length);
            }

            for (int i = 0; i < fieldReaders.length; i += 1) {
                int[] columns = mapping[i];
                FieldReader reader = fieldReaders[i];
                if (columns == null) {
                    reader.skip(decoder);
                } else {
                    Object value = reader.read(decoder, row.getField(columns[0]));
                    for (int column : columns) {
                        row.setField(column, value);
                    }
                }
            }
            return row;
        }

        @Override
        public void skip(Decoder decoder) throws IOException {
            for (FieldReader fieldReader : fieldReaders) {
                fieldReader.skip(decoder);
            }
        }
    }
}
