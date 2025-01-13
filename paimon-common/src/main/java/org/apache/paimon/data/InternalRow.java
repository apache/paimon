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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Base interface for an internal data structure representing data of {@link RowType}.
 *
 * <p>The mappings from SQL data types to the internal data structures are listed in the following
 * table:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL Data Types                 | Internal Data Structures                |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link BinaryString}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link Decimal}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (number of days since epoch)        |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (number of milliseconds of the day) |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | ROW                            | {@link InternalRow}                     |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link InternalArray}                   |
 * +--------------------------------+-----------------------------------------+
 * | MAP / MULTISET                 | {@link InternalMap}                     |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>Nullability is always handled by the container data structure.
 *
 * @see GenericRow
 * @see JoinedRow
 * @since 0.4.0
 */
@Public
public interface InternalRow extends DataGetters {

    /**
     * Returns the number of fields in this row.
     *
     * <p>The number does not include {@link RowKind}. It is kept separately.
     */
    int getFieldCount();

    /**
     * Returns the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    RowKind getRowKind();

    /**
     * Sets the kind of change that this row describes in a changelog.
     *
     * @see RowKind
     */
    void setRowKind(RowKind kind);

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /** Returns the data class for the given {@link DataType}. */
    static Class<?> getDataClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return Decimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.class;
            case ARRAY:
                return InternalArray.class;
            case MULTISET:
            case MAP:
                return InternalMap.class;
            case ROW:
                return InternalRow.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Creates an accessor for getting elements in an internal row data structure at the given
     * position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    static FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
                final int rowFieldCount = DataTypeChecks.getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case VARIANT:
                fieldGetter = row -> row.getVariant(fieldPos);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), InternalRow.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /** Accessor for getting the field of a row during runtime. */
    interface FieldGetter extends Serializable {
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }

    /**
     * Creates a {@link FieldSetter} for setting elements to a row from a row at the given position.
     *
     * @param fieldType the element type of the row
     * @param fieldPos the element position of the row
     */
    static FieldSetter createFieldSetter(DataType fieldType, int fieldPos) {
        final FieldSetter fieldSetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldSetter = (from, to) -> to.setBoolean(fieldPos, from.getBoolean(fieldPos));
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldSetter =
                        (from, to) ->
                                to.setDecimal(
                                        fieldPos,
                                        from.getDecimal(fieldPos, decimalPrecision, decimalScale),
                                        decimalPrecision);
                if (fieldType.isNullable() && !Decimal.isCompact(decimalPrecision)) {
                    return (from, to) -> {
                        if (from.isNullAt(fieldPos)) {
                            to.setNullAt(fieldPos);
                            to.setDecimal(fieldPos, null, decimalPrecision);
                        } else {
                            fieldSetter.setFieldFrom(from, to);
                        }
                    };
                }
                break;
            case TINYINT:
                fieldSetter = (from, to) -> to.setByte(fieldPos, from.getByte(fieldPos));
                break;
            case SMALLINT:
                fieldSetter = (from, to) -> to.setShort(fieldPos, from.getShort(fieldPos));
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldSetter = (from, to) -> to.setInt(fieldPos, from.getInt(fieldPos));
                break;
            case BIGINT:
                fieldSetter = (from, to) -> to.setLong(fieldPos, from.getLong(fieldPos));
                break;
            case FLOAT:
                fieldSetter = (from, to) -> to.setFloat(fieldPos, from.getFloat(fieldPos));
                break;
            case DOUBLE:
                fieldSetter = (from, to) -> to.setDouble(fieldPos, from.getDouble(fieldPos));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldSetter =
                        (from, to) ->
                                to.setTimestamp(
                                        fieldPos,
                                        from.getTimestamp(fieldPos, timestampPrecision),
                                        timestampPrecision);
                if (fieldType.isNullable() && !Timestamp.isCompact(timestampPrecision)) {
                    return (from, to) -> {
                        if (from.isNullAt(fieldPos)) {
                            to.setNullAt(fieldPos);
                            to.setTimestamp(fieldPos, null, timestampPrecision);
                        } else {
                            fieldSetter.setFieldFrom(from, to);
                        }
                    };
                }
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("type %s not support for setting", fieldType));
        }
        if (!fieldType.isNullable()) {
            return fieldSetter;
        }
        return (from, to) -> {
            if (from.isNullAt(fieldPos)) {
                to.setNullAt(fieldPos);
            } else {
                fieldSetter.setFieldFrom(from, to);
            }
        };
    }

    /** Accessor for setting the field of a row during runtime. */
    interface FieldSetter extends Serializable {
        void setFieldFrom(DataGetters from, DataSetters to);
    }
}
