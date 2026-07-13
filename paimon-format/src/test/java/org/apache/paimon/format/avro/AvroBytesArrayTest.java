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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroBytesArray}. */
public class AvroBytesArrayTest {

    @Test
    public void testNotNullElementsForSupportedTypes() throws Exception {
        checkResult(DataTypes.BOOLEAN(), objectArray(true, false, true), false);
        checkResult(DataTypes.TINYINT(), objectArray((byte) 1, (byte) 2, (byte) 3), false);
        checkResult(DataTypes.SMALLINT(), objectArray((short) 1, (short) 2, (short) 3), false);
        checkResult(DataTypes.INT(), objectArray(1, 2, 3), false);
        checkResult(DataTypes.BIGINT(), objectArray(1L, 2L, 3L), false);
        checkResult(DataTypes.FLOAT(), objectArray(1.25F, 2.5F, 3.75F), false);
        checkResult(DataTypes.DOUBLE(), objectArray(1.25D, 2.5D, 3.75D), false);
        checkResult(
                DataTypes.CHAR(10),
                objectArray(
                        BinaryString.fromString("a"),
                        BinaryString.fromString("b"),
                        BinaryString.fromString("c")),
                false);
        checkResult(
                DataTypes.STRING(),
                objectArray(
                        BinaryString.fromString("d"),
                        BinaryString.fromString("e"),
                        BinaryString.fromString("f")),
                false);
        checkResult(
                DataTypes.BINARY(10),
                objectArray(new byte[] {1, 2}, new byte[] {3, 4}, new byte[] {5, 6}),
                false);
        checkResult(
                DataTypes.VARBINARY(10),
                objectArray(new byte[] {1, 2}, new byte[] {3, 4}, new byte[] {5, 6}),
                false);
        checkResult(DataTypes.DATE(), objectArray(1, 2, 3), false);
        checkResult(DataTypes.TIME(), objectArray(1000, 2000, 3000), false);
        checkResult(
                DataTypes.TIMESTAMP(3),
                objectArray(
                        Timestamp.fromEpochMillis(1000),
                        Timestamp.fromEpochMillis(2000),
                        Timestamp.fromEpochMillis(3000)),
                false);
        checkResult(
                DataTypes.TIMESTAMP(6),
                objectArray(
                        Timestamp.fromMicros(1001),
                        Timestamp.fromMicros(2002),
                        Timestamp.fromMicros(3003)),
                false);
        checkResult(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                objectArray(
                        Timestamp.fromEpochMillis(1000),
                        Timestamp.fromEpochMillis(2000),
                        Timestamp.fromEpochMillis(3000)),
                false);
        checkResult(
                DataTypes.DECIMAL(10, 2),
                objectArray(decimal("1.23"), decimal("2.34"), decimal("3.45")),
                false);
    }

    @Test
    public void testNullableElementsForSupportedTypes() throws Exception {
        checkResult(DataTypes.BOOLEAN(), objectArray(true, null, false), true);
        checkResult(DataTypes.TINYINT(), objectArray((byte) 1, null, (byte) 2), true);
        checkResult(DataTypes.SMALLINT(), objectArray((short) 1, null, (short) 2), true);
        checkResult(DataTypes.INT(), objectArray(1, null, 2), true);
        checkResult(DataTypes.BIGINT(), objectArray(1L, null, 2L), true);
        checkResult(DataTypes.FLOAT(), objectArray(1.25F, null, 2.5F), true);
        checkResult(DataTypes.DOUBLE(), objectArray(1.25D, null, 2.5D), true);
        checkResult(
                DataTypes.CHAR(10),
                objectArray(BinaryString.fromString("a"), null, BinaryString.fromString("b")),
                true);
        checkResult(
                DataTypes.STRING(),
                objectArray(BinaryString.fromString("c"), null, BinaryString.fromString("d")),
                true);
        checkResult(
                DataTypes.BINARY(10),
                objectArray(new byte[] {1, 2}, null, new byte[] {3, 4}),
                true);
        checkResult(
                DataTypes.VARBINARY(10),
                objectArray(new byte[] {1, 2}, null, new byte[] {3, 4}),
                true);
        checkResult(DataTypes.DATE(), objectArray(1, null, 2), true);
        checkResult(DataTypes.TIME(), objectArray(1000, null, 2000), true);
        checkResult(
                DataTypes.TIMESTAMP(3),
                objectArray(Timestamp.fromEpochMillis(1000), null, Timestamp.fromEpochMillis(2000)),
                true);
        checkResult(
                DataTypes.TIMESTAMP(6),
                objectArray(Timestamp.fromMicros(1001), null, Timestamp.fromMicros(2002)),
                true);
        checkResult(
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                objectArray(Timestamp.fromEpochMillis(1000), null, Timestamp.fromEpochMillis(2000)),
                true);
        checkResult(
                DataTypes.DECIMAL(10, 2),
                objectArray(decimal("1.23"), null, decimal("2.34")),
                true);
    }

    private void checkResult(DataType elementType, GenericArray values, boolean nullable)
            throws Exception {
        DataType arrayElementType = nullable ? elementType : elementType.notNull();
        Schema arraySchema =
                AvroSchemaConverter.convertToSchema(
                        DataTypes.ARRAY(arrayElementType).notNull(), new HashMap<>());
        Schema elementSchema = arraySchema.getElementType();

        byte[] bytes = writeRawArrayBytes(arrayElementType, values);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        FieldReader elementReader = new FieldReaderFactory().visit(elementSchema, arrayElementType);
        AvroBytesArray array = AvroBytesArray.create(decoder, elementReader, elementSchema);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(values.size());

        checkArrayValues(elementType, values, array);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        encoder.writeArrayStart();
        encoder.setItemCount(array.size());
        array.writeRawElements(encoder, 0, array.size());
        encoder.writeArrayEnd();
        encoder.flush();
        assertThat(baos.toByteArray()).isEqualTo(bytes);
    }

    private byte[] writeRawArrayBytes(DataType elementType, GenericArray values) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
        encoder.writeArrayStart();
        encoder.setItemCount(values.size());
        for (int i = 0; i < values.size(); i++) {
            encoder.startItem();
            if (values.isNullAt(i)) {
                encoder.writeIndex(0);
            } else {
                if (elementType.isNullable()) {
                    encoder.writeIndex(1);
                }
                writeElement(
                        elementType,
                        InternalArray.createElementGetter(elementType).getElementOrNull(values, i),
                        encoder);
            }
        }
        encoder.writeArrayEnd();
        encoder.flush();
        return baos.toByteArray();
    }

    private void checkArrayValues(
            DataType elementType, GenericArray expectedValues, InternalArray actualArray) {
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
        for (int i = 0; i < expectedValues.size(); i++) {
            if (expectedValues.isNullAt(i)) {
                assertThat(actualArray.isNullAt(i)).isTrue();
            } else {
                checkValue(elementType, getter.getElementOrNull(expectedValues, i), actualArray, i);
            }
        }
    }

    private void checkValue(
            DataType elementType, Object expected, InternalArray actualArray, int pos) {
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                assertThat(actualArray.getString(pos).toString()).isEqualTo(expected.toString());
                return;
            case BOOLEAN:
                assertThat(actualArray.getBoolean(pos)).isEqualTo(expected);
                return;
            case BINARY:
            case VARBINARY:
                assertThat(actualArray.getBinary(pos)).containsExactly((byte[]) expected);
                return;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) elementType;
                assertThat(
                                actualArray.getDecimal(
                                        pos, decimalType.getPrecision(), decimalType.getScale()))
                        .isEqualTo(expected);
                return;
            case TINYINT:
                assertThat(actualArray.getByte(pos)).isEqualTo(expected);
                return;
            case SMALLINT:
                assertThat(actualArray.getShort(pos)).isEqualTo(expected);
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                assertThat(actualArray.getInt(pos)).isEqualTo(expected);
                return;
            case BIGINT:
                assertThat(actualArray.getLong(pos)).isEqualTo(expected);
                return;
            case FLOAT:
                assertThat(actualArray.getFloat(pos)).isEqualTo(expected);
                return;
            case DOUBLE:
                assertThat(actualArray.getDouble(pos)).isEqualTo(expected);
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assertThat(actualArray.getTimestamp(pos, timestampPrecision(elementType)))
                        .isEqualTo(expected);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported AvroBytesArray test type: " + elementType);
        }
    }

    private void writeElement(DataType elementType, Object value, BinaryEncoder encoder)
            throws Exception {
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                encoder.writeBoolean((Boolean) value);
                return;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                encoder.writeInt(((Number) value).intValue());
                return;
            case BIGINT:
                encoder.writeLong((Long) value);
                return;
            case FLOAT:
                encoder.writeFloat((Float) value);
                return;
            case DOUBLE:
                encoder.writeDouble((Double) value);
                return;
            case VARCHAR:
            case CHAR:
                encoder.writeString(value.toString());
                return;
            case BINARY:
            case VARBINARY:
                encoder.writeBytes((byte[]) value);
                return;
            case DECIMAL:
                encoder.writeBytes(((Decimal) value).toUnscaledBytes());
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp timestamp = (Timestamp) value;
                if (timestampPrecision(elementType) <= 3) {
                    encoder.writeLong(timestamp.getMillisecond());
                } else {
                    encoder.writeLong(timestamp.toMicros());
                }
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported E2E AvroBytesArray test type: " + elementType);
        }
    }

    private int timestampPrecision(DataType elementType) {
        switch (elementType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) elementType).getPrecision();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) elementType).getPrecision();
            default:
                throw new UnsupportedOperationException(
                        "Unsupported timestamp type: " + elementType);
        }
    }

    private GenericArray objectArray(Object... values) {
        return new GenericArray(values);
    }

    private Decimal decimal(String value) {
        return Decimal.fromBigDecimal(new BigDecimal(value), 10, 2);
    }
}
