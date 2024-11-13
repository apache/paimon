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

package org.apache.paimon.table;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.VarCharType;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Special fields in a {@link org.apache.paimon.types.RowType} with specific field ids.
 *
 * <p><b>System fields</b>:
 *
 * <ul>
 *   <li><code>_KEY_&lt;key-field&gt;</code>: Keys of a key-value. ID = 1073741823 + <code>
 *       (field-id)
 *       </code>.
 *   <li><code>_SEQUENCE_NUMBER</code>: Sequence number of a key-value. ID = 2147483646.
 *   <li><code>_VALUE_KIND</code>: Type of a key-value. See {@link org.apache.paimon.types.RowKind}.
 *       ID = 2147483645.
 *   <li><code>_LEVEL</code>: Which LSM tree level does this key-value stay in. ID = 2147483644.
 *   <li><code>rowkind</code>: THw rowkind field in audit-log system tables. ID = 2147483643.
 * </ul>
 *
 * <p><b>Structured type fields</b>:
 *
 * <p>These ids are mainly used as field ids in parquet files, so compute engines can read a field
 * directly by id. These ids are not stored in {@link org.apache.paimon.types.DataField}.
 *
 * <ul>
 *   <li>Array element field: ID = 536870911 + 1024 * <code>(array-field-id)</code> + depth.
 *   <li>Map key field: ID = 536870911 - 1024 * <code>(array-field-id)</code> - depth.
 *   <li>Map value field: ID = 536870911 + 1024 * <code>(array-field-id)</code> + depth.
 * </ul>
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>ARRAY(MAP(INT, ARRAY(INT))) type, outer array has field id 10, then map (element of outer
 *       array) has field id 536870911 + 1024 * 10 + 1, map key (int) has field id 536870911 - 1024
 *       * 10 - 2, map value (inner array) has field id 536870911 + 1024 * 10 + 2, inner array
 *       element (int) has field id 536870911 + 1024 * 10 + 3
 * </ul>
 */
public class SpecialFields {

    // ----------------------------------------------------------------------------------------
    // System fields
    // ----------------------------------------------------------------------------------------

    public static final int SYSTEM_FIELD_ID_START = Integer.MAX_VALUE / 2;

    public static final String KEY_FIELD_PREFIX = "_KEY_";
    public static final int KEY_FIELD_ID_START = SYSTEM_FIELD_ID_START;

    public static final DataField SEQUENCE_NUMBER =
            new DataField(Integer.MAX_VALUE - 1, "_SEQUENCE_NUMBER", DataTypes.BIGINT().notNull());

    public static final DataField VALUE_KIND =
            new DataField(Integer.MAX_VALUE - 2, "_VALUE_KIND", DataTypes.TINYINT().notNull());

    public static final DataField LEVEL =
            new DataField(Integer.MAX_VALUE - 3, "_LEVEL", DataTypes.INT().notNull());

    // only used by AuditLogTable
    public static final DataField ROW_KIND =
            new DataField(
                    Integer.MAX_VALUE - 4, "rowkind", new VarCharType(VarCharType.MAX_LENGTH));

    public static final Set<String> SYSTEM_FIELD_NAMES =
            Stream.of(SEQUENCE_NUMBER.name(), VALUE_KIND.name(), LEVEL.name(), ROW_KIND.name())
                    .collect(Collectors.toSet());

    public static boolean isSystemField(int fieldId) {
        return fieldId >= SYSTEM_FIELD_ID_START;
    }

    public static boolean isSystemField(String field) {
        return field.startsWith(KEY_FIELD_PREFIX) || SYSTEM_FIELD_NAMES.contains(field);
    }

    // ----------------------------------------------------------------------------------------
    // Structured type fields
    // ----------------------------------------------------------------------------------------

    public static final int STRUCTURED_TYPE_FIELD_ID_BASE = Integer.MAX_VALUE / 4;
    public static final int STRUCTURED_TYPE_FIELD_DEPTH_LIMIT = 1 << 10;

    public static int getArrayElementFieldId(int arrayFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                + arrayFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                + depth;
    }

    public static int getMapKeyFieldId(int mapFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                - mapFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                - depth;
    }

    public static int getMapValueFieldId(int mapFieldId, int depth) {
        return STRUCTURED_TYPE_FIELD_ID_BASE
                + mapFieldId * STRUCTURED_TYPE_FIELD_DEPTH_LIMIT
                + depth;
    }
}
