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
import org.apache.flink.table.functions.SqlLikeUtils;
import org.apache.flink.table.store.file.stats.FieldStats;

import javax.annotation.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link Predicate} to evaluate like. */
public class Like implements Predicate {

    private static final long serialVersionUID = 1L;

    /** Accepts simple LIKE patterns like "abc%". */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^%_]+)%");

    private final int index;

    private final Literal literal;

    @Nullable private final Literal escapeLiteral;

    public Like(int index, Literal literal, @Nullable Literal escapeLiteral) {
        this.index = index;
        this.literal = checkNotNull(literal);
        this.escapeLiteral = escapeLiteral;
    }

    @Override
    public boolean test(Object[] values) {
        return test(values[index]);
    }

    @Override
    public boolean test(long rowCount, FieldStats[] fieldStats) {
        FieldStats stats = fieldStats[index];
        if (rowCount == stats.nullCount()) {
            return false;
        }
        return test(stats.minValue()) || test(stats.maxValue());
    }

    private boolean test(Object object) {
        if (object instanceof BinaryStringData && literal.value() instanceof BinaryStringData) {
            BinaryStringData field = (BinaryStringData) object;
            BinaryStringData sqlPattern = (BinaryStringData) literal.value();
            String sqlPatternStr = sqlPattern.toString();
            BinaryStringData escape =
                    escapeLiteral == null ? null : (BinaryStringData) escapeLiteral.value();
            boolean allowQuick =
                    escape == null && !sqlPattern.contains(BinaryStringData.fromString("_"));
            if (allowQuick) {
                Matcher matcher = BEGIN_PATTERN.matcher(sqlPatternStr);
                if (matcher.matches()) {
                    return field.startsWith(BinaryStringData.fromString(matcher.group(1)));
                }
            } else {
                Pattern pattern =
                        Pattern.compile(
                                SqlLikeUtils.sqlToRegexLike(
                                        sqlPatternStr, escape == null ? null : escape.toString()));
                return pattern.matcher(field.toString()).matches();
            }
        }
        return false;
    }
}
