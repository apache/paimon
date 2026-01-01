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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;
import static org.apache.paimon.utils.InternalRowUtils.get;

/** Utilities to evaluate {@link Predicate} against {@link InternalRow} and stats. */
public class PredicateEvaluator {

    private PredicateEvaluator() {}

    private static final Cache<String, java.util.function.Predicate<String>> LIKE_CACHE =
            Caffeine.newBuilder().softValues().executor(Runnable::run).build();

    public static boolean test(Predicate predicate, InternalRow row) {
        if (predicate instanceof LeafPredicate) {
            return testLeaf((LeafPredicate) predicate, row);
        } else if (predicate instanceof TransformPredicate) {
            return testTransform((TransformPredicate) predicate, row);
        } else if (predicate instanceof CompoundPredicate) {
            return testCompound((CompoundPredicate) predicate, row);
        }
        throw new UnsupportedOperationException("Unknown predicate: " + predicate.getClass());
    }

    public static boolean test(
            Predicate predicate,
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts) {
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leaf = (LeafPredicate) predicate;
            Object min = get(minValues, leaf.index(), leaf.type());
            Object max = get(maxValues, leaf.index(), leaf.type());
            Long nullCount =
                    nullCounts.isNullAt(leaf.index()) ? null : nullCounts.getLong(leaf.index());
            if (nullCount == null || rowCount != nullCount) {
                if (min == null || max == null) {
                    return true;
                }
            }
            return testLeafFunction(
                    leaf.function(),
                    leaf.type(),
                    rowCount,
                    min,
                    max,
                    nullCount,
                    normalizeLiterals(leaf.type(), leaf.literals()));
        } else if (predicate instanceof TransformPredicate) {
            // Currently conservative.
            return true;
        } else if (predicate instanceof CompoundPredicate) {
            return testCompound(
                    (CompoundPredicate) predicate, rowCount, minValues, maxValues, nullCounts);
        }
        throw new UnsupportedOperationException("Unknown predicate: " + predicate.getClass());
    }

    private static boolean testCompound(CompoundPredicate predicate, InternalRow row) {
        CompoundPredicate.Function function = predicate.function();
        if (function instanceof And) {
            for (Predicate child : predicate.children()) {
                if (!test(child, row)) {
                    return false;
                }
            }
            return true;
        } else if (function instanceof Or) {
            for (Predicate child : predicate.children()) {
                if (test(child, row)) {
                    return true;
                }
            }
            return false;
        }
        throw new UnsupportedOperationException(
                "Unknown compound function: " + function.getClass());
    }

    private static boolean testCompound(
            CompoundPredicate predicate,
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts) {
        CompoundPredicate.Function function = predicate.function();
        if (function instanceof And) {
            for (Predicate child : predicate.children()) {
                if (!test(child, rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return true;
        } else if (function instanceof Or) {
            for (Predicate child : predicate.children()) {
                if (test(child, rowCount, minValues, maxValues, nullCounts)) {
                    return true;
                }
            }
            return false;
        }
        throw new UnsupportedOperationException(
                "Unknown compound function: " + function.getClass());
    }

    private static boolean testLeaf(LeafPredicate predicate, InternalRow row) {
        Object value = evalTransform(predicate.transform(), row);
        return testLeafFunction(
                predicate.function(),
                predicate.type(),
                value,
                normalizeLiterals(predicate.type(), predicate.literals()));
    }

    private static boolean testTransform(TransformPredicate predicate, InternalRow row) {
        Object value = evalTransform(predicate.transform(), row);
        DataType type = predicate.transform().outputType();
        return testLeafFunction(
                predicate.function, type, value, normalizeLiterals(type, predicate.literals));
    }

    private static boolean testLeafFunction(
            LeafFunction function, DataType type, Object field, List<Object> literals) {
        if (function instanceof LeafUnaryFunction) {
            return testUnaryFunction((LeafUnaryFunction) function, field);
        }

        if (function instanceof NullFalseLeafBinaryFunction) {
            Object literal = firstLiteral(literals);
            if (field == null || literal == null) {
                return false;
            }
            return testNullFalseBinaryFunction(
                    (NullFalseLeafBinaryFunction) function, type, field, literal);
        }

        if (function instanceof In) {
            return testIn(type, field, literals);
        }

        if (function instanceof NotIn) {
            return testNotIn(type, field, literals);
        }

        throw new UnsupportedOperationException("Unknown leaf function: " + function.getClass());
    }

    private static boolean testLeafFunction(
            LeafFunction function,
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (function instanceof LeafUnaryFunction) {
            return testUnaryFunctionStats((LeafUnaryFunction) function, rowCount, nullCount);
        }

        if (function instanceof NullFalseLeafBinaryFunction) {
            Object literal = firstLiteral(literals);
            if (nullCount != null) {
                if (rowCount == nullCount || literal == null) {
                    return false;
                }
            }
            return testNullFalseBinaryFunctionStats(
                    (NullFalseLeafBinaryFunction) function, type, min, max, literal);
        }

        if (function instanceof In) {
            return testInStats(type, rowCount, min, max, nullCount, literals);
        }

        if (function instanceof NotIn) {
            return testNotInStats(type, rowCount, min, max, nullCount, literals);
        }

        throw new UnsupportedOperationException("Unknown leaf function: " + function.getClass());
    }

    private static boolean testUnaryFunction(LeafUnaryFunction function, Object field) {
        if (function instanceof IsNull) {
            return field == null;
        } else if (function instanceof IsNotNull) {
            return field != null;
        }
        throw new UnsupportedOperationException("Unknown unary function: " + function.getClass());
    }

    private static boolean testUnaryFunctionStats(
            LeafUnaryFunction function, long rowCount, Long nullCount) {
        if (function instanceof IsNull) {
            return nullCount == null || nullCount > 0;
        } else if (function instanceof IsNotNull) {
            return nullCount == null || nullCount < rowCount;
        }
        throw new UnsupportedOperationException("Unknown unary function: " + function.getClass());
    }

    private static boolean testNullFalseBinaryFunction(
            NullFalseLeafBinaryFunction function, DataType type, Object field, Object literal) {
        if (function instanceof Equal) {
            return compareLiteral(type, literal, field) == 0;
        } else if (function instanceof NotEqual) {
            return compareLiteral(type, literal, field) != 0;
        } else if (function instanceof LessThan) {
            return compareLiteral(type, literal, field) > 0;
        } else if (function instanceof LessOrEqual) {
            return compareLiteral(type, literal, field) >= 0;
        } else if (function instanceof GreaterThan) {
            return compareLiteral(type, literal, field) < 0;
        } else if (function instanceof GreaterOrEqual) {
            return compareLiteral(type, literal, field) <= 0;
        } else if (function instanceof StartsWith) {
            return field.toString().startsWith(literal.toString());
        } else if (function instanceof EndsWith) {
            return field.toString().endsWith(literal.toString());
        } else if (function instanceof Contains) {
            return field.toString().contains(literal.toString());
        } else if (function instanceof Like) {
            return testLike(field, literal);
        }
        throw new UnsupportedOperationException("Unknown binary function: " + function.getClass());
    }

    private static boolean testNullFalseBinaryFunctionStats(
            NullFalseLeafBinaryFunction function,
            DataType type,
            Object min,
            Object max,
            Object literal) {
        if (function instanceof Equal) {
            return compareLiteral(type, literal, min) >= 0
                    && compareLiteral(type, literal, max) <= 0;
        } else if (function instanceof NotEqual) {
            return compareLiteral(type, literal, min) != 0
                    || compareLiteral(type, literal, max) != 0;
        } else if (function instanceof LessThan) {
            return compareLiteral(type, literal, min) > 0;
        } else if (function instanceof LessOrEqual) {
            return compareLiteral(type, literal, min) >= 0;
        } else if (function instanceof GreaterThan) {
            return compareLiteral(type, literal, max) < 0;
        } else if (function instanceof GreaterOrEqual) {
            return compareLiteral(type, literal, max) <= 0;
        } else if (function instanceof StartsWith) {
            // Same semantics as the original StartsWith stats evaluation.
            if (min == null || max == null) {
                return true;
            }
            String minStr = min.toString();
            String maxStr = max.toString();
            String pattern = literal.toString();
            return (minStr.startsWith(pattern) || minStr.compareTo(pattern) <= 0)
                    && (maxStr.startsWith(pattern) || maxStr.compareTo(pattern) >= 0);
        } else if (function instanceof EndsWith) {
            return true;
        } else if (function instanceof Contains) {
            return true;
        } else if (function instanceof Like) {
            return true;
        }
        throw new UnsupportedOperationException("Unknown binary function: " + function.getClass());
    }

    private static boolean testIn(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null && compareLiteral(type, literal, field) == 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean testInStats(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal != null
                    && compareLiteral(type, literal, min) >= 0
                    && compareLiteral(type, literal, max) <= 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean testNotIn(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null || compareLiteral(type, literal, field) == 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean testNotInStats(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null
                    || (compareLiteral(type, literal, min) == 0
                            && compareLiteral(type, literal, max) == 0)) {
                return false;
            }
        }
        return true;
    }

    private static boolean testLike(Object field, Object patternLiteral) {
        String pattern = patternLiteral.toString();
        java.util.function.Predicate<String> filter = LIKE_CACHE.getIfPresent(pattern);
        if (filter == null) {
            filter = createLikePredicate(patternLiteral);
            LIKE_CACHE.put(pattern, filter);
        }
        return filter.test(field.toString());
    }

    private static java.util.function.Predicate<String> createLikePredicate(Object patternLiteral) {
        Optional<Pair<NullFalseLeafBinaryFunction, Object>> optimized =
                LikeOptimization.tryOptimize(patternLiteral);
        if (optimized.isPresent()) {
            Object literal = optimized.get().getValue();
            NullFalseLeafBinaryFunction func = optimized.get().getKey();
            if (func instanceof Equal) {
                return s -> s.equals(literal.toString());
            } else if (func instanceof StartsWith) {
                return s -> s.startsWith(literal.toString());
            } else if (func instanceof EndsWith) {
                return s -> s.endsWith(literal.toString());
            } else if (func instanceof Contains) {
                return s -> s.contains(literal.toString());
            }
        }

        String regex = sqlToRegexLike(patternLiteral.toString(), null);
        Pattern compiled = Pattern.compile(regex);
        return input -> compiled.matcher(input).matches();
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

    private static Object firstLiteral(List<Object> literals) {
        Preconditions.checkNotNull(literals, "literals should not be null");
        Preconditions.checkArgument(!literals.isEmpty(), "literals should not be empty");
        return literals.get(0);
    }

    public static Object evalTransform(Transform transform, InternalRow row) {
        if (transform instanceof FieldTransform) {
            FieldRef fieldRef = ((FieldTransform) transform).fieldRef();
            return get(row, fieldRef.index(), fieldRef.type());
        } else if (transform instanceof CastTransform) {
            CastTransform castTransform = (CastTransform) transform;
            FieldRef fieldRef = (FieldRef) castTransform.inputs().get(0);
            Object input = get(row, fieldRef.index(), fieldRef.type());
            @SuppressWarnings("unchecked")
            CastExecutor<Object, Object> cast =
                    (CastExecutor<Object, Object>)
                            CastExecutors.resolve(fieldRef.type(), castTransform.outputType());
            if (cast == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot resolve cast executor from %s to %s",
                                fieldRef.type(), castTransform.outputType()));
            }
            return cast.cast(input);
        } else if (transform instanceof UpperTransform) {
            BinaryString s = evalStringInput(((UpperTransform) transform).inputs().get(0), row);
            return s == null ? null : s.toUpperCase();
        } else if (transform instanceof ConcatTransform) {
            List<Object> inputs = ((ConcatTransform) transform).inputs();
            List<BinaryString> strings = new ArrayList<>(inputs.size());
            for (Object input : inputs) {
                strings.add(evalStringInput(input, row));
            }
            return BinaryString.concat(strings);
        } else if (transform instanceof ConcatWsTransform) {
            List<Object> inputs = ((ConcatWsTransform) transform).inputs();
            Preconditions.checkArgument(inputs.size() >= 2, "concat_ws needs at least 2 inputs");
            BinaryString sep = evalStringInput(inputs.get(0), row);
            List<BinaryString> parts = new ArrayList<>();
            for (int i = 1; i < inputs.size(); i++) {
                BinaryString s = evalStringInput(inputs.get(i), row);
                if (s != null) {
                    parts.add(s);
                }
            }
            return BinaryString.concatWs(sep, parts);
        }
        throw new UnsupportedOperationException("Unsupported transform: " + transform.getClass());
    }

    @Nullable
    private static BinaryString evalStringInput(Object input, InternalRow row) {
        if (input == null) {
            return null;
        }
        if (input instanceof FieldRef) {
            FieldRef fieldRef = (FieldRef) input;
            int i = fieldRef.index();
            if (row.isNullAt(i)) {
                return null;
            }
            Object v = get(row, i, fieldRef.type());
            if (v == null) {
                return null;
            }
            if (v instanceof BinaryString) {
                return (BinaryString) v;
            }
            return BinaryString.fromString(v.toString());
        }
        if (input instanceof BinaryString) {
            return (BinaryString) input;
        }
        return BinaryString.fromString(input.toString());
    }

    private static List<Object> normalizeLiterals(DataType type, List<Object> literals) {
        if (literals == null || literals.isEmpty()) {
            return literals;
        }
        List<Object> converted = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            Object internal = PredicateUtils.convertJavaObject(type, literal);
            converted.add(InternalSerializers.create(type).copy(internal));
        }
        return converted;
    }
}
