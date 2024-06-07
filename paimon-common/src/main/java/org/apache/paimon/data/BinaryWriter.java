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

package org.apache.paimon.data;

import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;

/**
 * Writer to write a composite data format, like row, array. 1. Invoke {@link #reset()}. 2. Write
 * each field by writeXX or setNullAt. (Same field can not be written repeatedly.) 3. Invoke {@link
 * #complete()}.
 */
public interface BinaryWriter {

    /** Reset writer to prepare next write. */
    void reset();

    /** Set null to this field. */
    void setNullAt(int pos);

    void writeBoolean(int pos, boolean value);

    void writeByte(int pos, byte value);

    void writeShort(int pos, short value);

    void writeInt(int pos, int value);

    void writeLong(int pos, long value);

    void writeFloat(int pos, float value);

    void writeDouble(int pos, double value);

    void writeString(int pos, BinaryString value);

    void writeBinary(int pos, byte[] bytes);

    void writeDecimal(int pos, Decimal value, int precision);

    void writeTimestamp(int pos, Timestamp value, int precision);

    void writeArray(int pos, InternalArray value, InternalArraySerializer serializer);

    void writeMap(int pos, InternalMap value, InternalMapSerializer serializer);

    void writeRow(int pos, InternalRow value, InternalRowSerializer serializer);

    /** Finally, complete write to set real size to binary. */
    void complete();

    // --------------------------------------------------------------------------------------------

    /**
     * @deprecated Use {@code #createValueSetter(DataType)} for avoiding logical types during
     *     runtime.
     */
    @Deprecated
    static void write(
            BinaryWriter writer, int pos, Object o, DataType type, Serializer<?> serializer) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                writer.writeBoolean(pos, (boolean) o);
                break;
            case TINYINT:
                writer.writeByte(pos, (byte) o);
                break;
            case SMALLINT:
                writer.writeShort(pos, (short) o);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, (int) o);
                break;
            case BIGINT:
                writer.writeLong(pos, (long) o);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                writer.writeTimestamp(pos, (Timestamp) o, timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                writer.writeTimestamp(pos, (Timestamp) o, lzTs.getPrecision());
                break;
            case FLOAT:
                writer.writeFloat(pos, (float) o);
                break;
            case DOUBLE:
                writer.writeDouble(pos, (double) o);
                break;
            case CHAR:
            case VARCHAR:
                writer.writeString(pos, (BinaryString) o);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                writer.writeDecimal(pos, (Decimal) o, decimalType.getPrecision());
                break;
            case ARRAY:
                writer.writeArray(pos, (InternalArray) o, (InternalArraySerializer) serializer);
                break;
            case MAP:
            case MULTISET:
                writer.writeMap(pos, (InternalMap) o, (InternalMapSerializer) serializer);
                break;
            case ROW:
                writer.writeRow(pos, (InternalRow) o, (InternalRowSerializer) serializer);
                break;
            case BINARY:
            case VARBINARY:
                writer.writeBinary(pos, (byte[]) o);
                break;
            default:
                throw new UnsupportedOperationException("Not support type: " + type);
        }
    }

    /**
     * Creates an accessor for setting the elements of a binary writer during runtime.
     *
     * @param elementType the element type
     */
    static ValueSetter createValueSetter(DataType elementType) {
        return createValueSetter(elementType, null);
    }

    static ValueSetter createValueSetter(DataType elementType, Serializer<?> serializer) {
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return (writer, pos, value) -> writer.writeString(pos, (BinaryString) value);
            case BOOLEAN:
                return (writer, pos, value) -> writer.writeBoolean(pos, (boolean) value);
            case BINARY:
            case VARBINARY:
                return (writer, pos, value) -> writer.writeBinary(pos, (byte[]) value);
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeDecimal(pos, (Decimal) value, decimalPrecision);
            case TINYINT:
                return (writer, pos, value) -> writer.writeByte(pos, (byte) value);
            case SMALLINT:
                return (writer, pos, value) -> writer.writeShort(pos, (short) value);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return (writer, pos, value) -> writer.writeInt(pos, (int) value);
            case BIGINT:
                return (writer, pos, value) -> writer.writeLong(pos, (long) value);
            case FLOAT:
                return (writer, pos, value) -> writer.writeFloat(pos, (float) value);
            case DOUBLE:
                return (writer, pos, value) -> writer.writeDouble(pos, (double) value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                return (writer, pos, value) ->
                        writer.writeTimestamp(pos, (Timestamp) value, timestampPrecision);
            case ARRAY:
                final Serializer<?> arraySerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeArray(
                                pos,
                                (InternalArray) value,
                                (InternalArraySerializer) arraySerializer);
            case MULTISET:
            case MAP:
                final Serializer<?> mapSerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeMap(
                                pos, (InternalMap) value, (InternalMapSerializer) mapSerializer);
            case ROW:
                final Serializer<?> rowSerializer =
                        serializer == null ? InternalSerializers.create(elementType) : serializer;
                return (writer, pos, value) ->
                        writer.writeRow(
                                pos, (InternalRow) value, (InternalRowSerializer) rowSerializer);
            default:
                throw new IllegalArgumentException();
        }
    }

    /** Accessor for setting the elements of a binary writer during runtime. */
    interface ValueSetter extends Serializable {
        void setValue(BinaryWriter writer, int pos, Object value);
    }
}
