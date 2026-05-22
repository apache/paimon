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

package org.apache.paimon.format.row;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/** Reads rows from a decompressed block by local row index. */
class RowBlockReader {

    private final BlockInput buf;
    private final int rowCount;
    private final int offsetArrayStart;
    private final FieldReader[] fieldReaders;
    private final int headerSizeInBytes;

    RowBlockReader(BlockInput buf, RowType rowType) {
        this.buf = buf;
        int len = buf.data.length;
        this.rowCount = BlockInput.readIntLE(buf.data, len - 4);
        this.offsetArrayStart = len - 4 - rowCount * 4;

        int arity = rowType.getFieldCount();
        this.headerSizeInBytes = (arity + 7) / 8;
        this.fieldReaders = new FieldReader[arity];
        for (int i = 0; i < arity; i++) {
            fieldReaders[i] = createFieldReader(rowType.getTypeAt(i));
        }
    }

    int rowCount() {
        return rowCount;
    }

    InternalRow readRow(int localRowIndex) {
        buf.position = BlockInput.readIntLE(buf.data, offsetArrayStart + localRowIndex * 4);
        return readRow(headerSizeInBytes, fieldReaders);
    }

    // ======================== Row Reading ========================

    private InternalRow readRow(int headerSize, FieldReader[] readers) {
        int headerStart = buf.position;
        buf.position += headerSize;

        GenericRow row = new GenericRow(readers.length);
        for (int i = 0; i < readers.length; i++) {
            if ((buf.data[headerStart + i / 8] & (1 << (i % 8))) != 0) {
                row.setField(i, null);
            } else {
                row.setField(i, readers[i].read());
            }
        }
        return row;
    }

    // ======================== Field Reader Factory ========================

    private FieldReader createFieldReader(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return new StringFieldReader();
            case BOOLEAN:
                return new BooleanFieldReader();
            case BINARY:
            case VARBINARY:
                return new BinaryFieldReader();
            case DECIMAL:
                return new DecimalFieldReader(getPrecision(type), getScale(type));
            case TINYINT:
                return new TinyIntFieldReader();
            case SMALLINT:
                return new SmallIntFieldReader();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new IntFieldReader();
            case BIGINT:
                return new BigIntFieldReader();
            case FLOAT:
                return new FloatFieldReader();
            case DOUBLE:
                return new DoubleFieldReader();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampFieldReader(getPrecision(type));
            case VARIANT:
                return new VariantFieldReader();
            case BLOB:
                return new BlobFieldReader();
            case VECTOR:
                {
                    VectorType vectorType = (VectorType) type;
                    return new VectorFieldReader(vectorType.getElementType());
                }
            case ARRAY:
                {
                    DataType elementType = ((ArrayType) type).getElementType();
                    return new ArrayFieldReader(createFieldReader(elementType));
                }
            case MULTISET:
                {
                    DataType msElementType = ((MultisetType) type).getElementType();
                    return new MapFieldReader(
                            createFieldReader(msElementType), createFieldReader(new IntType()));
                }
            case MAP:
                {
                    MapType mapType = (MapType) type;
                    return new MapFieldReader(
                            createFieldReader(mapType.getKeyType()),
                            createFieldReader(mapType.getValueType()));
                }
            case ROW:
                {
                    RowType nestedType = (RowType) type;
                    return new RowFieldReader(nestedType);
                }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type.getTypeRoot());
        }
    }

    // ======================== Complex Types ========================

    private Object[] readElements(FieldReader elementReader) {
        int size = buf.readVarInt();
        int nullBitmapBytes = (size + 7) / 8;
        int nullStart = buf.position;
        buf.position += nullBitmapBytes;

        Object[] elements = new Object[size];
        for (int i = 0; i < size; i++) {
            if ((buf.data[nullStart + i / 8] & (1 << (i % 8))) != 0) {
                elements[i] = null;
            } else {
                elements[i] = elementReader.read();
            }
        }
        return elements;
    }

    private InternalArray readArray(FieldReader elementReader) {
        return new GenericArray(readElements(elementReader));
    }

    private InternalMap readMap(FieldReader keyReader, FieldReader valueReader) {
        Object[] keys = readElements(keyReader);
        Object[] values = readElements(valueReader);
        Map<Object, Object> map = new HashMap<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return new GenericMap(map);
    }

    // ======================== Interface ========================

    interface FieldReader {
        Object read();
    }

    // ======================== FieldReader Implementations ========================

    private class StringFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readString();
        }
    }

    private class BooleanFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readBoolean();
        }
    }

    private class BinaryFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readBytes();
        }
    }

    private class DecimalFieldReader implements FieldReader {
        private final int precision;
        private final int scale;

        DecimalFieldReader(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public Object read() {
            return buf.readDecimal(precision, scale);
        }
    }

    private class TinyIntFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readByte();
        }
    }

    private class SmallIntFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readShort();
        }
    }

    private class IntFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readInt();
        }
    }

    private class BigIntFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readLong();
        }
    }

    private class FloatFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readFloat();
        }
    }

    private class DoubleFieldReader implements FieldReader {
        @Override
        public Object read() {
            return buf.readDouble();
        }
    }

    private class TimestampFieldReader implements FieldReader {
        private final int precision;

        TimestampFieldReader(int precision) {
            this.precision = precision;
        }

        @Override
        public Object read() {
            return buf.readTimestamp(precision);
        }
    }

    private class VariantFieldReader implements FieldReader {
        @Override
        public Object read() {
            byte[] value = buf.readBytes();
            byte[] metadata = buf.readBytes();
            return new GenericVariant(value, metadata);
        }
    }

    private class BlobFieldReader implements FieldReader {
        @Override
        public Object read() {
            return Blob.fromData(buf.readBytes());
        }
    }

    private class VectorFieldReader implements FieldReader {
        private final DataType elementType;

        VectorFieldReader(DataType elementType) {
            this.elementType = elementType;
        }

        @Override
        public Object read() {
            int size = buf.readVarInt();
            InternalArray array = readVectorElements(size);
            return BinaryVector.fromInternalArray(array, elementType);
        }

        private InternalArray readVectorElements(int size) {
            switch (elementType.getTypeRoot()) {
                case BOOLEAN:
                    {
                        boolean[] arr = new boolean[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readBoolean();
                        }
                        return new GenericArray(arr);
                    }
                case TINYINT:
                    {
                        byte[] arr = new byte[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readByte();
                        }
                        return new GenericArray(arr);
                    }
                case SMALLINT:
                    {
                        short[] arr = new short[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readShort();
                        }
                        return new GenericArray(arr);
                    }
                case INTEGER:
                    {
                        int[] arr = new int[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readInt();
                        }
                        return new GenericArray(arr);
                    }
                case BIGINT:
                    {
                        long[] arr = new long[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readLong();
                        }
                        return new GenericArray(arr);
                    }
                case FLOAT:
                    {
                        float[] arr = new float[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readFloat();
                        }
                        return new GenericArray(arr);
                    }
                case DOUBLE:
                    {
                        double[] arr = new double[size];
                        for (int i = 0; i < size; i++) {
                            arr[i] = buf.readDouble();
                        }
                        return new GenericArray(arr);
                    }
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported vector element type: " + elementType);
            }
        }
    }

    private class ArrayFieldReader implements FieldReader {
        private final FieldReader elementReader;

        ArrayFieldReader(FieldReader elementReader) {
            this.elementReader = elementReader;
        }

        @Override
        public Object read() {
            return readArray(elementReader);
        }
    }

    private class MapFieldReader implements FieldReader {
        private final FieldReader keyReader;
        private final FieldReader valueReader;

        MapFieldReader(FieldReader keyReader, FieldReader valueReader) {
            this.keyReader = keyReader;
            this.valueReader = valueReader;
        }

        @Override
        public Object read() {
            return readMap(keyReader, valueReader);
        }
    }

    private class RowFieldReader implements FieldReader {
        private final int nestedHeaderSize;
        private final FieldReader[] nestedReaders;

        RowFieldReader(RowType nestedType) {
            int arity = nestedType.getFieldCount();
            this.nestedHeaderSize = (arity + 7) / 8;
            this.nestedReaders = new FieldReader[arity];
            for (int i = 0; i < arity; i++) {
                nestedReaders[i] = createFieldReader(nestedType.getTypeAt(i));
            }
        }

        @Override
        public Object read() {
            return readRow(nestedHeaderSize, nestedReaders);
        }
    }
}
