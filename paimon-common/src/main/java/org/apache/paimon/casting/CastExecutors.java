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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
     * If a field type is modified, pushing down a filter of it is dangerous. This method tries to
     * cast the literals of filter to its original type. It only cast the literals when the CastRule
     * is in whitelist. Otherwise, return Optional.empty().
     */
    public static Optional<List<Object>> castLiteralsWithEvolution(
            List<Object> literals, DataType inputType, DataType outputType) {
        if (inputType.equalsIgnoreNullable(outputType)) {
            return Optional.of(literals);
        }

        CastRule<?, ?> castRule = INSTANCE.internalResolve(inputType, outputType);
        if (castRule == null) {
            return Optional.empty();
        }

        if (castRule instanceof NumericPrimitiveCastRule) {
            // Ignore float literals because pushing down float filter result is unpredictable.
            // For example, (double) 0.1F in Java is 0.10000000149011612.

            if (inputType.is(DataTypeFamily.INTEGER_NUMERIC)
                    && outputType.is(DataTypeFamily.INTEGER_NUMERIC)) {
                // Ignore input scale < output scale because of overflow.
                // For example, alter 383 from INT to TINYINT, the query result is (byte) 383 ==
                // 127. If we push down filter f = 127, 383 will be filtered out mistakenly.

                if (integerScaleLargerThan(inputType.getTypeRoot(), outputType.getTypeRoot())) {
                    if (inputType.getTypeRoot() != DataTypeRoot.BIGINT) {
                        return Optional.of(literals);
                    }

                    // Parquet filter Int comparator cannot handle long value.
                    // See org.apache.parquet.schema.PrimitiveType.
                    // So ignore filter if long literal is out of int scale.
                    List<Object> newLiterals = new ArrayList<>(literals.size());
                    for (Object literal : literals) {
                        long originalValue = (long) literal;
                        int newValue = (int) originalValue;
                        if (originalValue != newValue) {
                            return Optional.empty();
                        }
                        newLiterals.add(newValue);
                    }
                    return Optional.of(newLiterals);
                }
            }
        }

        return Optional.empty();
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
