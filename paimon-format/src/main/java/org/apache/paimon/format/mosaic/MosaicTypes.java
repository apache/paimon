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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/** Recursive binary serialization/deserialization for {@link DataType}. */
public class MosaicTypes {

    private static final byte TYPE_BOOLEAN = 0;
    private static final byte TYPE_TINYINT = 1;
    private static final byte TYPE_SMALLINT = 2;
    private static final byte TYPE_INTEGER = 3;
    private static final byte TYPE_BIGINT = 4;
    private static final byte TYPE_FLOAT = 5;
    private static final byte TYPE_DOUBLE = 6;
    private static final byte TYPE_DATE = 7;
    private static final byte TYPE_CHAR = 8;
    private static final byte TYPE_VARCHAR = 9;
    private static final byte TYPE_STRING = 10;
    private static final byte TYPE_BINARY = 11;
    private static final byte TYPE_VARBINARY = 12;
    private static final byte TYPE_BYTES = 13;
    private static final byte TYPE_DECIMAL = 14;
    private static final byte TYPE_TIME = 15;
    private static final byte TYPE_TIMESTAMP = 16;
    private static final byte TYPE_TIMESTAMP_LTZ = 17;

    @FunctionalInterface
    interface TypeWriter {
        void write(DataOutputStream out, DataType type) throws IOException;
    }

    @FunctionalInterface
    interface TypeReader {
        DataType read(DataInputStream in, boolean nullable) throws IOException;
    }

    private static final TypeWriter[] WRITERS = new TypeWriter[DataTypeRoot.values().length];
    private static final TypeReader[] READERS = new TypeReader[18];

    static {
        // simple types
        reg(DataTypeRoot.BOOLEAN, TYPE_BOOLEAN, (in, n) -> new BooleanType(n));
        reg(DataTypeRoot.TINYINT, TYPE_TINYINT, (in, n) -> new TinyIntType(n));
        reg(DataTypeRoot.SMALLINT, TYPE_SMALLINT, (in, n) -> new SmallIntType(n));
        reg(DataTypeRoot.INTEGER, TYPE_INTEGER, (in, n) -> new IntType(n));
        reg(DataTypeRoot.BIGINT, TYPE_BIGINT, (in, n) -> new BigIntType(n));
        reg(DataTypeRoot.FLOAT, TYPE_FLOAT, (in, n) -> new FloatType(n));
        reg(DataTypeRoot.DOUBLE, TYPE_DOUBLE, (in, n) -> new DoubleType(n));
        reg(DataTypeRoot.DATE, TYPE_DATE, (in, n) -> new DateType(n));

        // CHAR
        WRITERS[DataTypeRoot.CHAR.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_CHAR);
                    out.writeBoolean(type.isNullable());
                    writeVarint(out, ((CharType) type).getLength());
                };
        READERS[TYPE_CHAR] = (in, n) -> new CharType(n, readVarint(in));

        // VARCHAR / STRING
        WRITERS[DataTypeRoot.VARCHAR.ordinal()] =
                (out, type) -> {
                    int len = ((VarCharType) type).getLength();
                    if (len == VarCharType.MAX_LENGTH) {
                        out.writeByte(TYPE_STRING);
                        out.writeBoolean(type.isNullable());
                    } else {
                        out.writeByte(TYPE_VARCHAR);
                        out.writeBoolean(type.isNullable());
                        writeVarint(out, len);
                    }
                };
        READERS[TYPE_VARCHAR] = (in, n) -> new VarCharType(n, readVarint(in));
        READERS[TYPE_STRING] = (in, n) -> new VarCharType(n, VarCharType.MAX_LENGTH);

        // BINARY
        WRITERS[DataTypeRoot.BINARY.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_BINARY);
                    out.writeBoolean(type.isNullable());
                    writeVarint(out, ((BinaryType) type).getLength());
                };
        READERS[TYPE_BINARY] = (in, n) -> new BinaryType(n, readVarint(in));

        // VARBINARY / BYTES
        WRITERS[DataTypeRoot.VARBINARY.ordinal()] =
                (out, type) -> {
                    int len = ((VarBinaryType) type).getLength();
                    if (len == VarBinaryType.MAX_LENGTH) {
                        out.writeByte(TYPE_BYTES);
                        out.writeBoolean(type.isNullable());
                    } else {
                        out.writeByte(TYPE_VARBINARY);
                        out.writeBoolean(type.isNullable());
                        writeVarint(out, len);
                    }
                };
        READERS[TYPE_VARBINARY] = (in, n) -> new VarBinaryType(n, readVarint(in));
        READERS[TYPE_BYTES] = (in, n) -> new VarBinaryType(n, VarBinaryType.MAX_LENGTH);

        // DECIMAL
        WRITERS[DataTypeRoot.DECIMAL.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_DECIMAL);
                    out.writeBoolean(type.isNullable());
                    DecimalType dt = (DecimalType) type;
                    writeVarint(out, dt.getPrecision());
                    writeVarint(out, dt.getScale());
                };
        READERS[TYPE_DECIMAL] = (in, n) -> new DecimalType(n, readVarint(in), readVarint(in));

        // TIME
        WRITERS[DataTypeRoot.TIME_WITHOUT_TIME_ZONE.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_TIME);
                    out.writeBoolean(type.isNullable());
                    writeVarint(out, ((TimeType) type).getPrecision());
                };
        READERS[TYPE_TIME] = (in, n) -> new TimeType(n, readVarint(in));

        // TIMESTAMP
        WRITERS[DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_TIMESTAMP);
                    out.writeBoolean(type.isNullable());
                    writeVarint(out, ((TimestampType) type).getPrecision());
                };
        READERS[TYPE_TIMESTAMP] = (in, n) -> new TimestampType(n, readVarint(in));

        // TIMESTAMP WITH LOCAL TIME ZONE
        WRITERS[DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE.ordinal()] =
                (out, type) -> {
                    out.writeByte(TYPE_TIMESTAMP_LTZ);
                    out.writeBoolean(type.isNullable());
                    writeVarint(out, ((LocalZonedTimestampType) type).getPrecision());
                };
        READERS[TYPE_TIMESTAMP_LTZ] = (in, n) -> new LocalZonedTimestampType(n, readVarint(in));
    }

    private static void reg(DataTypeRoot root, byte typeId, TypeReader reader) {
        WRITERS[root.ordinal()] =
                (out, type) -> {
                    out.writeByte(typeId);
                    out.writeBoolean(type.isNullable());
                };
        READERS[typeId] = reader;
    }

    public static void writeType(DataOutputStream out, DataType type) throws IOException {
        TypeWriter writer = WRITERS[type.getTypeRoot().ordinal()];
        if (writer == null) {
            throw new IOException("Unsupported Mosaic type: " + type.getTypeRoot());
        }
        writer.write(out, type);
    }

    public static DataType readType(DataInputStream in) throws IOException {
        int typeId = in.readByte() & 0xFF;
        boolean nullable = in.readBoolean();
        TypeReader reader = typeId < READERS.length ? READERS[typeId] : null;
        if (reader == null) {
            throw new IOException("Unsupported Mosaic type ID: " + typeId);
        }
        return reader.read(in, nullable);
    }
}
