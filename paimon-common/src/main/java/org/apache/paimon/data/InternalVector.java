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
import org.apache.paimon.types.VectorType;

/**
 * Base interface of an internal data structure representing data of {@link VectorType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * @see BinaryVector
 * @since 2.0.0
 */
@Public
public interface InternalVector extends InternalArray {

    // ------------------------------------------------------------------------------------------
    // Access Utilities
    // ------------------------------------------------------------------------------------------

    /**
     * Creates an accessor for getting elements in an internal vector data structure at the given
     * position.
     *
     * @param elementType the element type of the vector
     */
    static ElementGetter createElementGetter(DataType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                elementGetter = InternalArray::getBoolean;
                break;
            case TINYINT:
                elementGetter = InternalArray::getByte;
                break;
            case SMALLINT:
                elementGetter = InternalArray::getShort;
                break;
            case INTEGER:
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
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(),
                                InternalVector.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (vector, pos) -> {
            if (vector.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(vector, pos);
        };
    }
}
