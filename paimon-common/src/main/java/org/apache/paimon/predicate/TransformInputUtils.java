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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.types.DataTypeFamily.INTEGER_NUMERIC;
import static org.apache.paimon.utils.InternalRowUtils.get;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utilities shared by transforms with mixed field-reference and literal inputs. */
final class TransformInputUtils {

    private TransformInputUtils() {}

    static Object value(Object input, InternalRow row) {
        if (input instanceof FieldRef) {
            FieldRef ref = (FieldRef) input;
            return get(row, ref.index(), ref.type());
        }
        return input;
    }

    static BinaryString string(Object input, InternalRow row) {
        return (BinaryString) value(input, row);
    }

    static Integer integer(Object input, InternalRow row) {
        Object value = value(input, row);
        return value == null ? null : Math.toIntExact(((Number) value).longValue());
    }

    static void checkString(Object input, String message) {
        if (input instanceof FieldRef) {
            checkArgument(((FieldRef) input).type().is(CHARACTER_STRING), message);
        } else {
            checkArgument(input == null || input instanceof BinaryString, message);
        }
    }

    static void checkInteger(Object input, String message) {
        if (input instanceof FieldRef) {
            checkArgument(((FieldRef) input).type().is(INTEGER_NUMERIC), message);
        } else {
            checkArgument(input == null || input instanceof Number, message);
        }
    }

    static void checkDate(Object input, String message) {
        if (input instanceof FieldRef) {
            checkArgument(
                    ((FieldRef) input).type().getTypeRoot()
                            == org.apache.paimon.types.DataTypeRoot.DATE,
                    message);
        } else {
            checkArgument(input == null || input instanceof Integer, message);
        }
    }
}
