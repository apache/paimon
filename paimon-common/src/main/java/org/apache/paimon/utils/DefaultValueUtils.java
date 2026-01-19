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

package org.apache.paimon.utils;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Collections;

/** Utils for default value. */
public class DefaultValueUtils {

    private static final Variant NULL_VARIANT = GenericVariant.fromJson("null");

    public static Object convertDefaultValue(DataType dataType, String defaultValueStr) {
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolve =
                (CastExecutor<Object, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, dataType);

        if (resolve == null) {
            throw new RuntimeException("Default value do not support the type of " + dataType);
        }

        if (defaultValueStr.startsWith("'") && defaultValueStr.endsWith("'")) {
            defaultValueStr = defaultValueStr.substring(1, defaultValueStr.length() - 1);
        }

        return resolve.cast(BinaryString.fromString(defaultValueStr));
    }

    public static void validateDefaultValue(DataType dataType, @Nullable String defaultValueStr) {
        if (defaultValueStr == null) {
            return;
        }

        try {
            convertDefaultValue(dataType, defaultValueStr);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unsupported default value `" + defaultValueStr + "` for type " + dataType, e);
        }
    }

    /** Creates a default value object for the given {@link DataType}. */
    public static Object defaultValue(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return false;
            case TINYINT:
                return (byte) 0;
            case SMALLINT:
                return (short) 0;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 0;
            case BIGINT:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0d;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return Decimal.fromBigDecimal(
                        BigDecimal.ZERO, decimalType.getPrecision(), decimalType.getScale());
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString("");
            case BINARY:
                return new byte[((BinaryType) dataType).getLength()];
            case VARBINARY:
                return new byte[0];
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.fromEpochMillis(0);
            case ARRAY:
                return new GenericArray(new Object[0]);
            case MAP:
            case MULTISET:
                return new GenericMap(Collections.emptyMap());
            case ROW:
                RowType rowType = (RowType) dataType;
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    row.setField(i, defaultValue(rowType.getTypeAt(i)));
                }
                return row;
            case VARIANT:
                return NULL_VARIANT;
            case BLOB:
                return Blob.fromData(new byte[0]);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }
}
