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

package org.apache.paimon.format.orc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.orc.TypeDescription;

/** Util for orc types. */
public class OrcTypeUtil {

    public static final String PAIMON_ORC_FIELD_ID_KEY = "paimon.id";

    public static TypeDescription convertToOrcSchema(RowType rowType) {
        TypeDescription struct = TypeDescription.createStruct();
        for (DataField dataField : rowType.getFields()) {
            TypeDescription child = convertToOrcType(dataField.type(), dataField.id(), 0);
            struct.addField(dataField.name(), child);
        }
        return struct;
    }

    @VisibleForTesting
    static TypeDescription convertToOrcType(DataType type, int fieldId, int depth) {
        type = type.copy(true);
        switch (type.getTypeRoot()) {
            case CHAR:
                return TypeDescription.createChar()
                        .withMaxLength(((CharType) type).getLength())
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case VARCHAR:
                int len = ((VarCharType) type).getLength();
                if (len == VarCharType.MAX_LENGTH) {
                    return TypeDescription.createString()
                            .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
                } else {
                    return TypeDescription.createVarchar()
                            .withMaxLength(len)
                            .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
                }
            case BOOLEAN:
                return TypeDescription.createBoolean()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case VARBINARY:
                if (type.equals(DataTypes.BYTES())) {
                    return TypeDescription.createBinary()
                            .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
                } else {
                    throw new UnsupportedOperationException(
                            "Not support other binary type: " + type);
                }
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return TypeDescription.createDecimal()
                        .withScale(decimalType.getScale())
                        .withPrecision(decimalType.getPrecision())
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case TINYINT:
                return TypeDescription.createByte()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case SMALLINT:
                return TypeDescription.createShort()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
                return TypeDescription.createInt()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case BIGINT:
                return TypeDescription.createLong()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case FLOAT:
                return TypeDescription.createFloat()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case DOUBLE:
                return TypeDescription.createDouble()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case DATE:
                return TypeDescription.createDate()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TypeDescription.createTimestamp()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TypeDescription.createTimestampInstant()
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;

                String elementFieldId =
                        String.valueOf(SpecialFields.getArrayElementFieldId(fieldId, depth + 1));
                TypeDescription elementOrcType =
                        convertToOrcType(arrayType.getElementType(), fieldId, depth + 1)
                                .setAttribute(PAIMON_ORC_FIELD_ID_KEY, elementFieldId);

                return TypeDescription.createList(elementOrcType)
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case MAP:
                MapType mapType = (MapType) type;

                String mapKeyFieldId =
                        String.valueOf(SpecialFields.getMapKeyFieldId(fieldId, depth + 1));
                TypeDescription mapKeyOrcType =
                        convertToOrcType(mapType.getKeyType(), fieldId, depth + 1)
                                .setAttribute(PAIMON_ORC_FIELD_ID_KEY, mapKeyFieldId);

                String mapValueFieldId =
                        String.valueOf(SpecialFields.getMapValueFieldId(fieldId, depth + 1));
                TypeDescription mapValueOrcType =
                        convertToOrcType(mapType.getValueType(), fieldId, depth + 1)
                                .setAttribute(PAIMON_ORC_FIELD_ID_KEY, mapValueFieldId);

                return TypeDescription.createMap(mapKeyOrcType, mapValueOrcType)
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            case ROW:
                return convertToOrcSchema((RowType) type)
                        .setAttribute(PAIMON_ORC_FIELD_ID_KEY, String.valueOf(fieldId));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
