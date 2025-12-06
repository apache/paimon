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

import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.table.SpecialFields.ROW_ID;

/**
 * The {@link PredicateVisitor} to extract a list of Row IDs from predicates. The returned Row IDs
 * can be pushed down to manifest readers and file readers to enable efficient random access.
 *
 * <p>Note that there is a significant distinction between returning {@code null} and returning an
 * empty set:
 *
 * <ul>
 *   <li>{@code null} indicates that the predicate cannot be converted into a random-access pattern,
 *       meaning the filter is not consumable by this visitor.
 *   <li>An empty set indicates that no rows satisfy the predicate (e.g. {@code WHERE _ROW_ID = 3
 *       AND _ROW_ID IN (1, 2)}).
 * </ul>
 */
public class RowIdPredicateVisitor implements PredicateVisitor<Set<Long>> {

    @Override
    public Set<Long> visit(LeafPredicate predicate) {
        if (ROW_ID.name().equals(predicate.fieldName())) {
            LeafFunction function = predicate.function();
            if (function instanceof Equal || function instanceof In) {
                HashSet<Long> rowIds = new HashSet<>();
                for (Object literal : predicate.literals()) {
                    rowIds.add((Long) literal);
                }
                return rowIds;
            }
        }
        return null;
    }

    @Override
    public Set<Long> visit(CompoundPredicate predicate) {
        CompoundPredicate.Function function = predicate.function();
        HashSet<Long> rowIds = null;
        // `And` means we should get the intersection of all children.
        if (function instanceof And) {
            for (Predicate child : predicate.children()) {
                Set<Long> childSet = child.visit(this);
                if (childSet == null) {
                    return null;
                }

                if (rowIds == null) {
                    rowIds = new HashSet<>(childSet);
                } else {
                    rowIds.retainAll(childSet);
                }

                // shortcut for intersection
                if (rowIds.isEmpty()) {
                    return rowIds;
                }
            }
        } else if (function instanceof Or) {
            // `Or` means we should get the union of all children
            rowIds = new HashSet<>();
            for (Predicate child : predicate.children()) {
                Set<Long> childSet = child.visit(this);
                if (childSet == null) {
                    return null;
                }

                rowIds.addAll(childSet);
            }
        } else {
            // unexpected function type, just return null
            return null;
        }
        return rowIds;
    }

    @Override
    public Set<Long> visit(TransformPredicate predicate) {
        // do not support transform predicate now.
        return null;
    }
}
