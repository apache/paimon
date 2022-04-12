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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.store.file.stats.FieldStats;

/** A {@link Predicate} to evaluate {@code filter like 'abc%' or filter like 'abc_'}. */
public class StartsWith implements Predicate {

    private static final long serialVersionUID = 1L;

    private final int index;

    private final Literal patternLiteral;

    private final boolean matchOneCharacter;

    public StartsWith(int index, Literal patternLiteral, boolean matchOneCharacter) {
        this.index = index;
        this.patternLiteral = patternLiteral;
        this.matchOneCharacter = matchOneCharacter;
    }

    @Override
    public boolean test(Object[] values) {
        BinaryStringData field = (BinaryStringData) values[index];
        if (field != null && field.startsWith((BinaryStringData) patternLiteral.value())) {
            return !matchOneCharacter
                    || field.numChars() - ((BinaryStringData) patternLiteral.value()).numChars()
                            == 1;
        }
        return false;
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        FieldStats stats = fieldStats[index];
        if (rowCount == stats.nullCount()) {
            return false;
        }
        BinaryStringData min = (BinaryStringData) stats.minValue();
        BinaryStringData max = (BinaryStringData) stats.maxValue();
        BinaryStringData pattern = (BinaryStringData) patternLiteral.value();
        return (min.startsWith(pattern) || min.compareTo(pattern) <= 0)
                && (max.startsWith(pattern) || max.compareTo(pattern) >= 0);
    }
}
