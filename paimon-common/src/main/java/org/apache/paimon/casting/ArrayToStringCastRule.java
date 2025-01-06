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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VarCharType;

/** {@link DataTypeRoot#ARRAY} to {@link DataTypeFamily#CHARACTER_STRING} cast rule. */
public class ArrayToStringCastRule extends AbstractCastRule<InternalArray, BinaryString> {

    static final ArrayToStringCastRule INSTANCE = new ArrayToStringCastRule();

    private ArrayToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.ARRAY)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalArray, BinaryString> create(
            DataType inputType, DataType targetType) {
        ArrayType arrayType = (ArrayType) inputType;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(arrayType.getElementType());
        CastExecutor castExecutor =
                CastExecutors.resolve(arrayType.getElementType(), VarCharType.STRING_TYPE);

        return arrayData -> {
            int size = arrayData.size();
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < size; i++) {
                Object o = elementGetter.getElementOrNull(arrayData, i);
                if (o == null) {
                    sb.append("null");
                } else {
                    sb.append(castExecutor.cast(o));
                }
                if (i != size - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            return BinaryString.fromString(sb.toString());
        };
    }
}
