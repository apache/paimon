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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An inclusive {@link PredicateVisitor} that projects predicates for a table's data rows into
 * predicates on a projected schema (e.g. partition values).
 *
 * <p>The visitor is inclusive: if the original predicate matches a row, then the projected
 * predicate will match that row's projected values. In other words, the projected predicate is a
 * necessary condition (superset) of the original — it may have false positives but never false
 * negatives.
 *
 * <p>Projection semantics by node type:
 *
 * <ul>
 *   <li><b>Leaf:</b> projectable iff all referenced fields exist in the projection. Returns the
 *       predicate with remapped field indices, or empty if any field is outside the projection.
 *   <li><b>AND:</b> projects each child independently; keeps only projectable children and drops
 *       the rest. This is safe because if {@code AND(A,B)} is true then A is true, so A's
 *       projection is true. Returns empty if no children are projectable.
 *   <li><b>OR:</b> all children must be projectable. If any child cannot be projected, we cannot
 *       guarantee inclusiveness (that child might match a row whose projected values don't satisfy
 *       any projected branch). Returns empty if any child fails.
 * </ul>
 */
public class PredicateProjectionConverter implements PredicateVisitor<Optional<Predicate>> {

    private final Map<Integer, Integer> reversed;

    private PredicateProjectionConverter(Map<Integer, Integer> reversed) {
        this.reversed = reversed;
    }

    /**
     * Creates a converter from a projection array.
     *
     * @param projection array where {@code projection[projectedIndex] = originalIndex}
     */
    public static PredicateProjectionConverter fromProjection(int[] projection) {
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < projection.length; i++) {
            mapping.put(projection[i], i);
        }
        return new PredicateProjectionConverter(mapping);
    }

    /**
     * Creates a converter from a direct field index mapping.
     *
     * @param fieldIdxMapping array where {@code fieldIdxMapping[originalIndex] = projectedIndex},
     *     or a negative value if the field is not in the projection
     */
    public static PredicateProjectionConverter fromMapping(int[] fieldIdxMapping) {
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int i = 0; i < fieldIdxMapping.length; i++) {
            if (fieldIdxMapping[i] >= 0) {
                mapping.put(i, fieldIdxMapping[i]);
            }
        }
        return new PredicateProjectionConverter(mapping);
    }

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        List<Object> inputs = predicate.transform().inputs();
        List<Object> newInputs = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof FieldRef) {
                FieldRef fieldRef = (FieldRef) input;
                Integer mappedIndex = reversed.get(fieldRef.index());
                if (mappedIndex != null) {
                    newInputs.add(new FieldRef(mappedIndex, fieldRef.name(), fieldRef.type()));
                } else {
                    return Optional.empty();
                }
            } else {
                newInputs.add(input);
            }
        }
        return Optional.of(predicate.copyWithNewInputs(newInputs));
    }

    @Override
    public Optional<Predicate> visit(CompoundPredicate predicate) {
        List<Predicate> converted = new ArrayList<>();
        boolean isAnd = predicate.function() instanceof And;
        for (Predicate child : predicate.children()) {
            Optional<Predicate> optional = child.visit(this);
            if (optional.isPresent()) {
                converted.add(optional.get());
            } else {
                if (!isAnd) {
                    // OR: all children must be projectable
                    return Optional.empty();
                }
                // AND: skip non-projectable children (inclusive)
            }
        }
        if (converted.isEmpty()) {
            return Optional.empty();
        }
        if (converted.size() == 1) {
            return Optional.of(converted.get(0));
        }
        return Optional.of(new CompoundPredicate(predicate.function(), converted));
    }
}
