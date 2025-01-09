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
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.VarCharType;

/** {@link DataTypeRoot#MAP} to {@link DataTypeFamily#CHARACTER_STRING} cast rule. */
public class MapToStringCastRule extends AbstractCastRule<InternalMap, BinaryString> {

    static final MapToStringCastRule INSTANCE = new MapToStringCastRule();

    private MapToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.MAP)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalMap, BinaryString> create(DataType inputType, DataType targetType) {
        MapType mapType = (MapType) inputType;
        InternalArray.ElementGetter keyGetter =
                InternalArray.createElementGetter(mapType.getKeyType());
        InternalArray.ElementGetter valueGetter =
                InternalArray.createElementGetter(mapType.getValueType());
        CastExecutor keyCastExecutor =
                CastExecutors.resolve(mapType.getKeyType(), VarCharType.STRING_TYPE);
        CastExecutor valueCastExecutor =
                CastExecutors.resolve(mapType.getValueType(), VarCharType.STRING_TYPE);

        return mapData -> {
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            int size = mapData.size();
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < size; i++) {
                Object k = keyGetter.getElementOrNull(keyArray, i);
                if (k == null) {
                    sb.append("null");
                } else {
                    sb.append(keyCastExecutor.cast(k));
                }
                sb.append(" -> ");
                Object v = valueGetter.getElementOrNull(valueArray, i);
                if (v == null) {
                    sb.append("null");
                } else {
                    sb.append(valueCastExecutor.cast(v));
                }
                if (i != size - 1) {
                    sb.append(", ");
                }
            }
            sb.append("}");
            return BinaryString.fromString(sb.toString());
        };
    }
}
