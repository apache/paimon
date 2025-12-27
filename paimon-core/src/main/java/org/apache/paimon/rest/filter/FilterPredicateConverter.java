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

package org.apache.paimon.rest.filter;

import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Converter from REST {@link Filter} to Paimon {@link Predicate}. */
public class FilterPredicateConverter {

    private FilterPredicateConverter() {}

    @Nullable
    public static Predicate toPredicate(@Nullable Filter filter) {
        if (filter == null) {
            return null;
        }

        if (filter instanceof CompoundFilter) {
            CompoundFilter cp = (CompoundFilter) filter;
            List<Predicate> children =
                    cp.children() == null
                            ? new ArrayList<>()
                            : cp.children().stream()
                                    .map(FilterPredicateConverter::toPredicate)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
            CompoundPredicate.Function fn =
                    cp.function() == CompoundFilterFunction.OR ? Or.INSTANCE : And.INSTANCE;
            return new CompoundPredicate(fn, children);
        }

        if (filter instanceof TransformFilter) {
            TransformFilter tp = (TransformFilter) filter;
            Transform transform = toTransform(tp.transform());
            LeafFunction fn = toLeafFunction(tp.function());
            List<Object> literals = tp.literals() == null ? new ArrayList<>() : tp.literals();
            List<Object> converted = new ArrayList<>(literals.size());
            DataType literalType = transform.outputType();
            for (Object o : literals) {
                converted.add(PredicateBuilder.convertJavaObject(literalType, o));
            }
            return TransformPredicate.of(transform, fn, converted);
        }

        throw new IllegalArgumentException(
                "Unsupported filter type: " + filter.getClass().getName());
    }

    private static Transform toTransform(FilterTransform transform) {
        if (transform instanceof FieldFilterTransform) {
            FieldFilterTransform ft = (FieldFilterTransform) transform;
            return new FieldTransform(new FieldRef(ft.index(), ft.name(), ft.dataType()));
        }
        throw new IllegalArgumentException(
                "Unsupported transform type: " + transform.getClass().getName());
    }

    private static LeafFunction toLeafFunction(LeafFilterFunction function) {
        switch (function) {
            case EQUAL:
                return Equal.INSTANCE;
            case NOT_EQUAL:
                return NotEqual.INSTANCE;
            case GREATER_THAN:
                return GreaterThan.INSTANCE;
            case GREATER_OR_EQUAL:
                return GreaterOrEqual.INSTANCE;
            case LESS_THAN:
                return LessThan.INSTANCE;
            case LESS_OR_EQUAL:
                return LessOrEqual.INSTANCE;
            case IN:
                return In.INSTANCE;
            case NOT_IN:
                return NotIn.INSTANCE;
            case IS_NULL:
                return IsNull.INSTANCE;
            case IS_NOT_NULL:
                return IsNotNull.INSTANCE;
            case STARTS_WITH:
                return StartsWith.INSTANCE;
            case ENDS_WITH:
                return EndsWith.INSTANCE;
            case CONTAINS:
                return Contains.INSTANCE;
            case LIKE:
                return Like.INSTANCE;
            default:
                throw new IllegalArgumentException("Unsupported leaf function: " + function);
        }
    }
}
