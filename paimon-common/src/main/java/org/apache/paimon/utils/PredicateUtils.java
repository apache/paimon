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

package org.apache.paimon.utils;

import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Utils for {@link Predicate}. */
public class PredicateUtils {

    /**
     * Try to rewrite possible {@code GREATER_OR_EQUAL} and {@code LESS_OR_EQUAL} predicates to
     * {@code BETWEEN} leaf predicate. This method will recursively try to rewrite the children
     * predicates of an {@code AND}, for example: {@code OR(a >= 1, AND(b >= 1, b <= 2))} will be
     * rewritten to {@code OR(a >= 1, BETWEEN(b, 1, 2))}.
     */
    public static Predicate tryRewriteBetweenPredicate(@Nullable Predicate filter) {
        if (filter == null || filter instanceof LeafPredicate) {
            return filter;
        }
        CompoundPredicate compoundPredicate = (CompoundPredicate) filter;
        boolean isOr = compoundPredicate.function() instanceof Or;

        Map<Transform, List<LeafPredicate>> leavesByTransform = new HashMap<>();
        List<Predicate> resultChildren = new ArrayList<>();
        // Flatten the children predicates of an AND
        // For example, for AND(b >= 1, AND(a >= 1, b <= 2)), we will get [a >= 1, b >= 1, b <= 2]
        // After flattening, all children will be either LeafPredicate or ORPredicate
        List<Predicate> effectiveChildren =
                isOr ? compoundPredicate.children() : flattenChildren(compoundPredicate.children());
        for (Predicate child : effectiveChildren) {
            if (child instanceof LeafPredicate) {
                leavesByTransform
                        .computeIfAbsent(
                                ((LeafPredicate) child).transform(), k -> new ArrayList<>())
                        .add((LeafPredicate) child);
            } else {
                resultChildren.add(tryRewriteBetweenPredicate(child));
            }
        }

        for (Map.Entry<Transform, List<LeafPredicate>> leaves : leavesByTransform.entrySet()) {
            if (isOr) {
                resultChildren.addAll(leaves.getValue());
                continue;
            }

            Transform transform = leaves.getKey();

            // for children predicates of an AND, we only need to reserve
            // the largest GREATER_OR_EQUAL and the smallest LESS_OR_EQUAL
            // For example, for AND(a >= 1, a >= 2, a <= 3, a <= 4), we only need to reserve a >= 2
            // and a <= 3
            LeafPredicate gtePredicate = null;
            LeafPredicate ltePredicate = null;
            for (LeafPredicate leaf : leaves.getValue()) {
                if (leaf.function() instanceof GreaterOrEqual) {
                    if (gtePredicate == null
                            || CompareUtils.compareLiteral(
                                            transform.outputType(),
                                            leaf.literals().get(0),
                                            gtePredicate.literals().get(0))
                                    > 0) {
                        gtePredicate = leaf;
                    }
                } else if (leaf.function() instanceof LessOrEqual) {
                    if (ltePredicate == null
                            || CompareUtils.compareLiteral(
                                            transform.outputType(),
                                            leaf.literals().get(0),
                                            ltePredicate.literals().get(0))
                                    < 0) {
                        ltePredicate = leaf;
                    }
                } else {
                    resultChildren.add(leaf);
                }
            }

            boolean converted = false;
            if (gtePredicate != null && ltePredicate != null) {
                Optional<Predicate> betweenLeaf = convertToBetweenLeaf(gtePredicate, ltePredicate);
                if (betweenLeaf.isPresent()) {
                    converted = true;
                    resultChildren.add(betweenLeaf.get());
                }
            }
            if (!converted) {
                if (gtePredicate != null) {
                    resultChildren.add(gtePredicate);
                }
                if (ltePredicate != null) {
                    resultChildren.add(ltePredicate);
                }
            }
        }

        return isOr ? PredicateBuilder.or(resultChildren) : PredicateBuilder.and(resultChildren);
    }

    private static List<Predicate> flattenChildren(List<Predicate> children) {
        List<Predicate> result = new ArrayList<>();
        for (Predicate child : children) {
            if (child instanceof LeafPredicate) {
                result.add(child);
            } else {
                CompoundPredicate compoundPredicate = (CompoundPredicate) child;
                if (compoundPredicate.function() instanceof Or) {
                    result.add(child);
                } else {
                    result.addAll(flattenChildren(compoundPredicate.children()));
                }
            }
        }
        return result;
    }

    /**
     * Convert child predicates of an AND to a BETWEEN leaf predicate. Return `Optional.empty()` if
     * not possible.
     */
    public static Optional<Predicate> convertToBetweenLeaf(
            Predicate leftChild, Predicate rightChild) {
        if (leftChild instanceof LeafPredicate && rightChild instanceof LeafPredicate) {
            LeafPredicate left = (LeafPredicate) leftChild;
            LeafPredicate right = (LeafPredicate) rightChild;
            if (Objects.equals(left.transform(), right.transform())) {
                if (left.function() instanceof GreaterOrEqual
                        && right.function() instanceof LessOrEqual) {
                    return createBetweenLeaf(left, right);
                } else if (left.function() instanceof LessOrEqual
                        && right.function() instanceof GreaterOrEqual) {
                    return createBetweenLeaf(right, left);
                }
            }
        }

        return Optional.empty();
    }

    private static Optional<Predicate> createBetweenLeaf(
            LeafPredicate gtePredicate, LeafPredicate ltePredicate) {
        // gtePredicate and ltePredicate should have the same transform
        Transform transform = gtePredicate.transform();
        Object lbLiteral = gtePredicate.literals().get(0);
        Object ubLiteral = ltePredicate.literals().get(0);

        if (CompareUtils.compareLiteral(transform.outputType(), lbLiteral, ubLiteral) > 0) {
            return Optional.empty();
        }

        return Optional.of(
                new LeafPredicate(
                        transform, Between.INSTANCE, Arrays.asList(lbLiteral, ubLiteral)));
    }
}
