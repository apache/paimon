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

package org.apache.paimon.data.variant;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Test helper for building {@link GenericVariant} values from typed Java objects. */
public final class GenericVariantBuilderHelper {

    private GenericVariantBuilderHelper() {}

    public static GenericVariant build(RowType rowType, Map<String, Object> values) {
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        appendValue(builder, rowType, values);
        return builder.result();
    }

    /** Build a variant object from a RowType schema and a map of field values. */
    private static void appendObject(
            GenericVariantBuilder builder, RowType rowType, Map<String, Object> values) {
        int start = builder.getWritePos();
        ArrayList<GenericVariantBuilder.FieldEntry> fields = new ArrayList<>();
        for (DataField field : rowType.getFields()) {
            String key = field.name();
            if (!values.containsKey(key)) {
                continue;
            }
            fields.add(
                    new GenericVariantBuilder.FieldEntry(
                            key, builder.addKey(key), builder.getWritePos() - start));
            Object value = values.get(key);
            if (value == null) {
                builder.appendNull();
            } else {
                appendValue(builder, field.type(), value);
            }
        }
        builder.finishWritingObject(start, fields);
    }

    /** Build a variant array from an element type and a list of element values. */
    private static void appendArray(
            GenericVariantBuilder builder, DataType elementType, List<Object> values) {
        int start = builder.getWritePos();
        ArrayList<Integer> offsets = new ArrayList<>();
        for (Object value : values) {
            offsets.add(builder.getWritePos() - start);
            if (value == null) {
                builder.appendNull();
            } else {
                appendValue(builder, elementType, value);
            }
        }
        builder.finishWritingArray(start, offsets);
    }

    @SuppressWarnings("unchecked")
    private static void appendValue(
            GenericVariantBuilder builder, DataType dataType, Object value) {
        switch (dataType.getTypeRoot()) {
            case VARCHAR:
            case CHAR:
                builder.appendString((String) value);
                break;
            case TINYINT:
                builder.appendLong((Byte) value);
                break;
            case SMALLINT:
                builder.appendLong((Short) value);
                break;
            case INTEGER:
                builder.appendLong((Integer) value);
                break;
            case BIGINT:
                builder.appendLong((Long) value);
                break;
            case FLOAT:
                builder.appendFloat((Float) value);
                break;
            case DOUBLE:
                builder.appendDouble((Double) value);
                break;
            case BOOLEAN:
                builder.appendBoolean((Boolean) value);
                break;
            case BINARY:
            case VARBINARY:
                builder.appendBinary((byte[]) value);
                break;
            case DECIMAL:
                builder.appendDecimal((BigDecimal) value);
                break;
            case DATE:
                builder.appendDate((Integer) value);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                builder.appendTimestamp((Long) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                builder.appendTimestampNtz((Long) value);
                break;
            case ROW:
                appendObject(builder, (RowType) dataType, (Map<String, Object>) value);
                break;
            case ARRAY:
                appendArray(builder, ((ArrayType) dataType).getElementType(), (List<Object>) value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + dataType);
        }
    }
}
