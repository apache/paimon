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

import org.apache.paimon.types.DataType;

import java.util.List;

/** Field and literals in this function can't be null. */
public abstract class NullFalseLeafFunction extends LeafFunction {
    private static final long serialVersionUID = 1L;

    public abstract boolean test0(DataType type, Object field, List<Object> literals);

    public abstract boolean test0(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals);

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }

        // Only when the count of literals is one we can return quickly checking for null
        if (literals.size() == 1 && literals.get(0) == null) {
            return false;
        }

        return test0(type, field, literals);
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        // All null
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }

        // Only when the count of literals is one we can quickly check for null
        if (literals.size() == 1 && literals.get(0) == null) {
            return false;
        }

        // todo: support one of min and max is not null
        if (min == null || max == null) {
            return true;
        }

        return test0(type, rowCount, min, max, nullCount, literals);
    }
}
