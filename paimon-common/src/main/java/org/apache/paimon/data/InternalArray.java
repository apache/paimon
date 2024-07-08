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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getFieldCount;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/**
 * Base interface of an internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * @see GenericArray
 * @since 0.4.0
 */
@Public
public interface InternalArray extends DataGetters {

    /** Returns the number of elements in this array. */
    int size();

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    boolean[] toBooleanArray();

    byte[] toByteArray();

    short[] toShortArray();

    int[] toIntArray();

    long[] toLongArray();

    float[] toFloatArray();

    double[] toDoubleArray();

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal array data structure at the given
     * position.
     *
     * @param elementType the element type of the array
     */
    static ElementGetter createElementGetter(DataType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                elementGetter = InternalArray::getString;
                break;
            case BOOLEAN:
                elementGetter = InternalArray::getBoolean;
                break;
            case BINARY:
            case VARBINARY:
                elementGetter = InternalArray::getBinary;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                elementGetter =
                        (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = InternalArray::getByte;
                break;
            case SMALLINT:
                elementGetter = InternalArray::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = InternalArray::getInt;
                break;
            case BIGINT:
                elementGetter = InternalArray::getLong;
                break;
            case FLOAT:
                elementGetter = InternalArray::getFloat;
                break;
            case DOUBLE:
                elementGetter = InternalArray::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                elementGetter = (array, pos) -> array.getTimestamp(pos, timestampPrecision);
                break;
            case ARRAY:
                elementGetter = InternalArray::getArray;
                break;
            case MULTISET:
            case MAP:
                elementGetter = InternalArray::getMap;
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(elementType);
                elementGetter = (array, pos) -> array.getRow(pos, rowFieldCount);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(),
                                InternalArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * Accessor for getting the elements of an array during runtime.
     *
     * @see #createElementGetter(DataType)
     */
    interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(InternalArray array, int pos);
    }
}
