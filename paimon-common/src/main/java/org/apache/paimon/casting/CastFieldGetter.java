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

import org.apache.paimon.data.InternalRow;

/**
 * Get field value from row with given pos and cast it according to specific {@link CastExecutor}.
 */
public class CastFieldGetter {

    private final InternalRow.FieldGetter fieldGetter;
    private final CastExecutor<Object, Object> castExecutor;
    private final String fieldName;

    @SuppressWarnings("unchecked")
    public CastFieldGetter(
            InternalRow.FieldGetter fieldGetter,
            CastExecutor<?, ?> castExecutor,
            String fieldName) {
        this.fieldGetter = fieldGetter;
        this.castExecutor = (CastExecutor<Object, Object>) castExecutor;
        this.fieldName = fieldName;
    }

    @SuppressWarnings("unchecked")
    public CastFieldGetter(InternalRow.FieldGetter fieldGetter, CastExecutor<?, ?> castExecutor) {
        this(fieldGetter, castExecutor, null);
    }

    @SuppressWarnings("unchecked")
    public <V> V getFieldOrNull(InternalRow row) {
        Object value = fieldGetter.getFieldOrNull(row);
        if (value == null) {
            return null;
        }
        try {
            return (V) castExecutor.cast(value);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to cast value for field '%s': %s",
                            fieldName != null ? fieldName : "<unknown>", e.getMessage()),
                    e);
        }
    }
}
