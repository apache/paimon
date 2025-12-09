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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/** A {@link NullFalseLeafBinaryFunction} to evaluate {@code filter like}. */
public class Like extends NullFalseLeafBinaryFunction {

    public static final Like INSTANCE = new Like();

    private static final Cache<BinaryString, Filter<BinaryString>> CACHE =
            Caffeine.newBuilder().softValues().executor(Runnable::run).build();

    private Like() {}

    @Override
    public boolean test(DataType type, Object field, Object patternLiteral) {
        if (field == null) {
            return false;
        }

        BinaryString pattern = (BinaryString) patternLiteral;
        Filter<BinaryString> filter = CACHE.getIfPresent(pattern);
        if (filter == null) {
            filter = createFunc(type, patternLiteral);
            CACHE.put(pattern, filter);
        }
        return filter.test((BinaryString) field);
    }

    private Filter<BinaryString> createFunc(DataType type, Object patternLiteral) {
        Optional<Pair<NullFalseLeafBinaryFunction, Object>> optimized =
                LikeOptimization.tryOptimize(patternLiteral);
        if (optimized.isPresent()) {
            NullFalseLeafBinaryFunction func = optimized.get().getKey();
            Object literal = optimized.get().getValue();
            return field -> func.test(type, field, literal);
        }
        // TODO optimize for chain checkers when there is no '_'
        // TODO for example: "abc%def%","%abc%def","%abc%def%","abc%def"
        String regex = sqlToRegexLike(patternLiteral.toString(), null);
        Pattern pattern = Pattern.compile(regex);
        return input -> pattern.matcher(input.toString()).matches();
    }

    private static String sqlToRegexLike(String sqlPattern, @Nullable CharSequence escapeStr) {
        char escapeChar;
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw invalidEscapeCharacter(escapeStr.toString());
            }

            escapeChar = escapeStr.charAt(0);
        } else {
            escapeChar = '\\';
        }

        return sqlToRegexLike(sqlPattern, escapeChar);
    }

    private static String sqlToRegexLike(String sqlPattern, char escapeChar) {
        int len = sqlPattern.length();
        StringBuilder javaPattern = new StringBuilder(len + len);

        for (int i = 0; i < len; ++i) {
            char c = sqlPattern.charAt(i);
            if ("[]()|^-+*?{}$\\.".indexOf(c) >= 0) {
                javaPattern.append('\\');
            }

            if (c == escapeChar) {
                if (i == sqlPattern.length() - 1) {
                    throw invalidEscapeSequence(sqlPattern, i);
                }

                char nextChar = sqlPattern.charAt(i + 1);
                if (nextChar != '_' && nextChar != '%' && nextChar != escapeChar) {
                    throw invalidEscapeSequence(sqlPattern, i);
                }

                javaPattern.append(nextChar);
                ++i;
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append("(?s:.*)");
            } else {
                javaPattern.append(c);
            }
        }

        return javaPattern.toString();
    }

    private static RuntimeException invalidEscapeCharacter(String s) {
        return new RuntimeException("Invalid escape character '" + s + "'");
    }

    private static RuntimeException invalidEscapeSequence(String s, int i) {
        return new RuntimeException("Invalid escape sequence '" + s + "', " + i);
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            Object patternLiteral) {
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitLike(fieldRef, literals.get(0));
    }
}
