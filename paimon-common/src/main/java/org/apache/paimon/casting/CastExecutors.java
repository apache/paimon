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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Cast executors for input type and output type. */
public class CastExecutors {

    /* ------- Singleton declaration ------- */

    private static final CastExecutors INSTANCE = new CastExecutors();

    static {
        INSTANCE
                // Numeric rules
                .addRule(DecimalToDecimalCastRule.INSTANCE)
                .addRule(NumericPrimitiveToDecimalCastRule.INSTANCE)
                .addRule(DecimalToNumericPrimitiveCastRule.INSTANCE)
                .addRule(NumericPrimitiveCastRule.INSTANCE)
                .addRule(NumericPrimitiveToTimestamp.INSTANCE)
                // Boolean <-> numeric rules
                .addRule(BooleanToNumericCastRule.INSTANCE)
                .addRule(NumericToBooleanCastRule.INSTANCE)
                // To string rules
                .addRule(NumericToStringCastRule.INSTANCE)
                .addRule(BooleanToStringCastRule.INSTANCE)
                .addRule(TimestampToStringCastRule.INSTANCE)
                .addRule(TimeToStringCastRule.INSTANCE)
                .addRule(DateToStringCastRule.INSTANCE)
                .addRule(StringToStringCastRule.INSTANCE)
                // From string rules
                .addRule(StringToBooleanCastRule.INSTANCE)
                .addRule(StringToDecimalCastRule.INSTANCE)
                .addRule(StringToNumericPrimitiveCastRule.INSTANCE)
                .addRule(StringToDateCastRule.INSTANCE)
                .addRule(StringToTimeCastRule.INSTANCE)
                .addRule(StringToTimestampCastRule.INSTANCE)
                .addRule(StringToBinaryCastRule.INSTANCE)
                // Date/Time/Timestamp rules
                .addRule(TimestampToTimestampCastRule.INSTANCE)
                .addRule(TimestampToDateCastRule.INSTANCE)
                .addRule(TimestampToTimeCastRule.INSTANCE)
                .addRule(DateToTimestampCastRule.INSTANCE)
                .addRule(TimeToTimestampCastRule.INSTANCE)
                .addRule(TimestampToNumericPrimitiveCastRule.INSTANCE)
                // To binary rules
                .addRule(BinaryToBinaryCastRule.INSTANCE)
                .addRule(BinaryToStringCastRule.INSTANCE);
    }

    /* ------- Entrypoint ------- */

    private static final CastExecutor<?, ?> IDENTITY_CAST_EXECUTOR = value -> value;

    /**
     * Resolve a {@link CastExecutor} for the provided input type and target type. Returns null if
     * no rule can be resolved.
     *
     * @param inputType the input value type.
     * @param outputType the output value type.
     * @return the {@link CastExecutor} instance.
     */
    public static @Nullable CastExecutor<?, ?> resolve(DataType inputType, DataType outputType) {
        CastRule<?, ?> rule = INSTANCE.internalResolve(inputType, outputType);
        if (rule == null) {
            return null;
        }
        return rule.create(inputType, outputType);
    }

    public static CastExecutor<?, ?> identityCastExecutor() {
        return IDENTITY_CAST_EXECUTOR;
    }

    /**
     * When filter a field witch was evolved from/to a numeric type, we should carefully handle the
     * precision match and overflow problem.
     */
    @Nullable
    public static List<Object> safelyCastLiteralsWithNumericEvolution(
            LeafPredicate predicate, DataType outputType) {
        DataType inputType = predicate.type();
        if (inputType.equalsIgnoreNullable(outputType)) {
            return predicate.literals();
        }

        List<Object> literals = predicate.literals();

        CastRule<?, ?> castRule = INSTANCE.internalResolve(inputType, outputType);
        if (castRule == null) {
            return literals;
        }

        if (castRule instanceof DecimalToDecimalCastRule) {
            if (((DecimalType) inputType).getPrecision() < ((DecimalType) outputType).getPrecision()
                    && containsEqualCheck(predicate)) {
                // For example, alter 111.321 from DECIMAL(6, 3) to DECIMAL(5, 2).
                // The query result is 111.32 which is truncated from 111.321.
                // If we query with filter f = 111.32 and push down it, 111.321 will be filtered
                // out mistakenly.
                // But if we query with filter f > 111.32, although 111.321 will be retrieved,
                // the engine will filter out it finally.
                return null;
            }
            // Pushing down higher precision filter is always correct.
            return literals;
        } else if (castRule instanceof NumericPrimitiveToDecimalCastRule) {
            if (inputType.is(DataTypeFamily.INTEGER_NUMERIC) && containsEqualCheck(predicate)) {
                // the reason is same as DecimalToDecimalCastRule
                return null;
            }
            return literals.stream()
                    .map(literal -> (Number) literal)
                    .map(
                            literal ->
                                    inputType.is(DataTypeFamily.INTEGER_NUMERIC)
                                            ? BigDecimal.valueOf(literal.longValue())
                                            : BigDecimal.valueOf(literal.doubleValue()))
                    .map(bd -> Decimal.fromBigDecimal(bd, bd.precision(), bd.scale()))
                    .collect(Collectors.toList());
        } else if (castRule instanceof DecimalToNumericPrimitiveCastRule) {
            if (outputType.is(DataTypeFamily.INTEGER_NUMERIC)
                    && (containsPartialCheck(predicate) || containsNotEqualCheck(predicate))) {
                // For example, alter 111 from INT to DECIMAL(5, 2). The query result is 111.00
                // If we query with filter f < 111.01 and push down it as f < 111, 111 will be
                // filtered out mistakenly. Also, we shouldn't push down f <> 111.01.
                // But if we query with filter f = 111.01 and push down it as f = 111, although 111
                // will be retrieved, the engine will filter out it finally.
                // TODO: maybe we can scale the partial filter. For example, f < 111.01 can be
                // transfer to f < 112.
                return null;
            } else if (outputType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                    && containsEqualCheck(predicate)) {
                // For example, alter 111.321 from DOUBLE to DECIMAL(5, 2). The query result is
                // 111.32.
                // If we query with filter f = 111.32 and push down it, 111.321 will be filtered
                // out mistakenly.
                // But if we query with filter f > 111.32 or f <> 111.32, although 111.321 will be
                // retrieved, the engine will filter out it finally.
                return null;
            }
            castLiterals(castRule, inputType, outputType, literals);
        } else if (castRule instanceof NumericPrimitiveCastRule) {
            if (inputType.is(DataTypeFamily.INTEGER_NUMERIC)
                    && outputType.is(DataTypeFamily.INTEGER_NUMERIC)) {
                if (integerScaleLargerThan(inputType.getTypeRoot(), outputType.getTypeRoot())) {
                    // Pushing down higher scale integer numeric filter is always correct.
                    return literals;
                }
            }

            // Pushing down float filter is dangerous because the filter result is unpredictable.
            // For example, (double) 0.1F in Java is 0.10000000149011612.

            // Pushing down lower scale filter is also dangerous because of overflow.
            // For example, alter 383 from INT to TINYINT, the query result is (byte) 383 == 127.
            // If we push down filter f = 127, 383 will be filtered out which is wrong.

            // So we don't push down these filters.
            return null;
        } else if (castRule instanceof NumericToStringCastRule
                || castRule instanceof StringToDecimalCastRule
                || castRule instanceof StringToNumericPrimitiveCastRule) {
            // Pushing down filters related to STRING is dangerous because string comparison is
            // different from number comparison and string literal to number might have precision
            // and overflow problem.
            // For example, alter '111' from STRING to INT, the query result is 111.
            // If we query with filter f > 2 and push down it as f > '2', '111' will be filtered
            // out mistakenly.
            return null;
        }

        // Non numeric related cast rule
        return castLiterals(castRule, inputType, outputType, literals);
    }

    private static List<Object> castLiterals(
            CastRule<?, ?> castRule,
            DataType inputType,
            DataType outputType,
            List<Object> literals) {
        CastExecutor<Object, Objects> castExecutor =
                (CastExecutor<Object, Objects>) castRule.create(inputType, outputType);
        return literals.stream()
                .map(l -> castExecutor == null ? l : castExecutor.cast(l))
                .collect(Collectors.toList());
    }

    private static boolean containsEqualCheck(LeafPredicate predicate) {
        LeafFunction function = predicate.function();
        return function instanceof In
                || function instanceof Equal
                || function instanceof GreaterOrEqual
                || function instanceof LessOrEqual;
    }

    private static boolean containsPartialCheck(LeafPredicate predicate) {
        LeafFunction function = predicate.function();
        return function instanceof LessThan
                || function instanceof LessOrEqual
                || function instanceof GreaterThan
                || function instanceof GreaterOrEqual;
    }

    private static boolean containsNotEqualCheck(LeafPredicate predicate) {
        LeafFunction function = predicate.function();
        return function instanceof NotIn || function instanceof NotEqual;
    }

    private static boolean integerScaleLargerThan(DataTypeRoot a, DataTypeRoot b) {
        return (a == DataTypeRoot.SMALLINT && b == DataTypeRoot.TINYINT)
                || (a == DataTypeRoot.INTEGER && b != DataTypeRoot.BIGINT)
                || a == DataTypeRoot.BIGINT;
    }

    // Map<Target family or root, Map<Input family or root, rule>>
    private final Map<Object, Map<Object, CastRule<?, ?>>> rules = new HashMap<>();

    private CastExecutors addRule(CastRule<?, ?> rule) {
        CastRulePredicate predicate = rule.getPredicateDefinition();

        for (DataType targetType : predicate.getTargetTypes()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetType, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (DataTypeRoot targetTypeRoot : predicate.getTargetTypeRoots()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeRoot, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (DataTypeFamily targetTypeFamily : predicate.getTargetTypeFamilies()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeFamily, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }

        return this;
    }

    private CastRule<?, ?> internalResolve(DataType inputType, DataType targetType) {

        final Iterator<Object> targetTypeRootFamilyIterator =
                Stream.concat(
                                Stream.of(targetType),
                                Stream.<Object>concat(
                                        Stream.of(targetType.getTypeRoot()),
                                        targetType.getTypeRoot().getFamilies().stream()))
                        .iterator();

        // Try lookup by target type root/type families
        while (targetTypeRootFamilyIterator.hasNext()) {
            final Object targetMapKey = targetTypeRootFamilyIterator.next();
            final Map<Object, CastRule<?, ?>> inputTypeToCastRuleMap = rules.get(targetMapKey);

            if (inputTypeToCastRuleMap == null) {
                continue;
            }

            // Try lookup by input type root/type families
            Optional<? extends CastRule<?, ?>> rule =
                    Stream.<Object>concat(
                                    Stream.of(inputType.getTypeRoot()),
                                    inputType.getTypeRoot().getFamilies().stream())
                            .map(inputTypeToCastRuleMap::get)
                            .filter(Objects::nonNull)
                            .findFirst();

            if (rule.isPresent()) {
                return rule.get();
            }
        }

        return null;
    }
}
