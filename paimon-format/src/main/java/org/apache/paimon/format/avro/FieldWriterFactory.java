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

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

/** Factory to create {@link FieldWriter}. */
public class FieldWriterFactory implements AvroSchemaVisitor<FieldWriter> {

    private static final FieldWriter STRING_WRITER =
            (container, i, encoder) ->
                    encoder.writeString(new Utf8(container.getString(i).toBytes()));

    private static final FieldWriter BYTES_WRITER =
            (container, i, encoder) -> encoder.writeBytes(container.getBinary(i));

    private static final FieldWriter BOOLEAN_WRITER =
            (container, i, encoder) -> encoder.writeBoolean(container.getBoolean(i));

    private static final FieldWriter INT_WRITER =
            (container, i, encoder) -> encoder.writeInt(container.getInt(i));

    private static final FieldWriter TINYINT_WRITER =
            (container, i, encoder) -> encoder.writeInt(container.getByte(i));

    private static final FieldWriter SMALLINT_WRITER =
            (container, i, encoder) -> encoder.writeInt(container.getShort(i));

    private static final FieldWriter BIGINT_WRITER =
            (container, i, encoder) -> encoder.writeLong(container.getLong(i));

    private static final FieldWriter FLOAT_WRITER =
            (container, i, encoder) -> encoder.writeFloat(container.getFloat(i));

    private static final FieldWriter DOUBLE_WRITER =
            (container, i, encoder) -> encoder.writeDouble(container.getDouble(i));

    @Override
    public FieldWriter visitUnion(Schema schema, DataType type) {
        return new NullableWriter(visit(schema.getTypes().get(1), type));
    }

    @Override
    public FieldWriter visitString() {
        return STRING_WRITER;
    }

    @Override
    public FieldWriter visitBytes() {
        return BYTES_WRITER;
    }

    @Override
    public FieldWriter visitInt() {
        return INT_WRITER;
    }

    @Override
    public FieldWriter visitTinyInt() {
        return TINYINT_WRITER;
    }

    @Override
    public FieldWriter visitSmallInt() {
        return SMALLINT_WRITER;
    }

    @Override
    public FieldWriter visitBoolean() {
        return BOOLEAN_WRITER;
    }

    @Override
    public FieldWriter visitBigInt() {
        return BIGINT_WRITER;
    }

    @Override
    public FieldWriter visitFloat() {
        return FLOAT_WRITER;
    }

    @Override
    public FieldWriter visitDouble() {
        return DOUBLE_WRITER;
    }

    @Override
    public FieldWriter visitTimestampMillis(Integer precision) {
        if (precision == null) {
            throw new AvroRuntimeException("Can't assign null when creating FieldWriter");
        }
        return (container, i, encoder) ->
                encoder.writeLong(container.getTimestamp(i, precision).getMillisecond());
    }

    @Override
    public FieldWriter visitTimestampMicros(Integer precision) {
        if (precision == null) {
            throw new AvroRuntimeException("Can't assign null when creating FieldWriter");
        }
        return (container, i, encoder) ->
                encoder.writeLong(container.getTimestamp(i, precision).toMicros());
    }

    @Override
    public FieldWriter visitDecimal(Integer precision, Integer scale) {
        if (precision == null || scale == null) {
            throw new AvroRuntimeException("Can't assign null when creating FieldWriter");
        }
        return (container, index, encoder) -> {
            Decimal decimal = container.getDecimal(index, precision, scale);
            encoder.writeBytes(decimal.toUnscaledBytes());
        };
    }

    @Override
    public FieldWriter visitArray(Schema schema, DataType elementType) {
        FieldWriter elementWriter = visit(schema.getElementType(), elementType);
        return (container, index, encoder) -> {
            InternalArray array = container.getArray(index);
            encoder.writeArrayStart();
            int numElements = array.size();
            encoder.setItemCount(numElements);
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                elementWriter.write(array, i, encoder);
            }
            encoder.writeArrayEnd();
        };
    }

    @Override
    public FieldWriter visitArrayMap(Schema schema, DataType keyType, DataType valueType) {
        RowWriter entryWriter =
                new RowWriter(
                        schema.getElementType(),
                        RowType.of(
                                        new DataType[] {keyType, valueType},
                                        new String[] {"key", "value"})
                                .getFields());
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        return (container, index, encoder) -> {
            InternalMap map = container.getMap(index);
            encoder.writeArrayStart();
            int numElements = map.size();
            InternalArray keyArray = map.keyArray();
            InternalArray valueArray = map.valueArray();
            encoder.setItemCount(numElements);
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                entryWriter.writeRow(
                        GenericRow.of(
                                keyGetter.getElementOrNull(keyArray, i),
                                valueGetter.getElementOrNull(valueArray, i)),
                        encoder);
            }
            encoder.writeArrayEnd();
        };
    }

    @Override
    public FieldWriter visitMap(Schema schema, DataType valueType) {
        FieldWriter valueWriter = visit(schema.getValueType(), valueType);
        return (container, index, encoder) -> {
            InternalMap map = container.getMap(index);
            encoder.writeMapStart();
            int numElements = map.size();
            encoder.setItemCount(numElements);
            InternalArray keyArray = map.keyArray();
            InternalArray valueArray = map.valueArray();
            for (int i = 0; i < numElements; i += 1) {
                encoder.startItem();
                STRING_WRITER.write(keyArray, i, encoder);
                valueWriter.write(valueArray, i, encoder);
            }
            encoder.writeMapEnd();
        };
    }

    @Override
    public FieldWriter visitRecord(Schema schema, @NotNull List<DataField> fields) {
        return new RowWriter(schema, fields);
    }

    private static class NullableWriter implements FieldWriter {

        private final FieldWriter writer;

        public NullableWriter(FieldWriter writer) {
            this.writer = writer;
        }

        @Override
        public void write(DataGetters container, int index, Encoder encoder) throws IOException {
            if (container.isNullAt(index)) {
                encoder.writeIndex(0);
            } else {
                encoder.writeIndex(1);
                writer.write(container, index, encoder);
            }
        }
    }

    /** A {@link FieldWriter} to write {@link InternalRow}. */
    public class RowWriter implements FieldWriter {

        private final FieldWriter[] fieldWriters;

        private RowWriter(Schema schema, List<DataField> fields) {
            List<Schema.Field> schemaFields = schema.getFields();
            this.fieldWriters = new FieldWriter[schemaFields.size()];
            for (int i = 0, fieldsSize = schemaFields.size(); i < fieldsSize; i++) {
                Schema.Field field = schemaFields.get(i);
                DataType type = fields.get(i).type();
                fieldWriters[i] = visit(field.schema(), type);
            }
        }

        @Override
        public void write(DataGetters container, int index, Encoder encoder) throws IOException {
            InternalRow row = container.getRow(index, fieldWriters.length);
            writeRow(row, encoder);
        }

        public void writeRow(InternalRow row, Encoder encoder) throws IOException {
            for (int i = 0; i < fieldWriters.length; i += 1) {
                fieldWriters[i].write(row, i, encoder);
            }
        }
    }

    public RowWriter createRowWriter(Schema schema, List<DataField> fields) {
        return new RowWriter(schema, fields);
    }
}
