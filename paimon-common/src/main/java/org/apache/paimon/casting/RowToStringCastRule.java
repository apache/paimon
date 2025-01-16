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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InternalRowUtils;

/** {@link DataTypeRoot#ROW} to {@link DataTypeFamily#CHARACTER_STRING} cast rule. */
public class RowToStringCastRule extends AbstractCastRule<InternalRow, BinaryString> {

    static final RowToStringCastRule INSTANCE = new RowToStringCastRule();

    private RowToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.ROW)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalRow, BinaryString> create(DataType inputType, DataType targetType) {
        RowType rowType = (RowType) inputType;
        int fieldCount = rowType.getFieldCount();
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldCount];
        CastExecutor[] castExecutors = new CastExecutor[fieldCount];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType fieldType = rowType.getTypeAt(i);
            fieldGetters[i] = InternalRowUtils.createNullCheckingFieldGetter(fieldType, i);
            castExecutors[i] = CastExecutors.resolve(fieldType, VarCharType.STRING_TYPE);
        }

        return rowDate -> {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < fieldCount; i++) {
                Object field = fieldGetters[i].getFieldOrNull(rowDate);
                if (field == null) {
                    sb.append("null");
                } else {
                    sb.append(castExecutors[i].cast(field).toString());
                }
                if (i != fieldCount - 1) {
                    sb.append(", ");
                }
            }
            sb.append("}");
            return BinaryString.fromString(sb.toString());
        };
    }
}
