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

/** System fields. */
public class SystemFields {

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
}
