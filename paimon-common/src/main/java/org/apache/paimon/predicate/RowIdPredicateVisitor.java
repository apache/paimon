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

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.table.SpecialFields.ROW_ID;

/**
 * The {@link PredicateVisitor} to extract a list of row id ranges from predicates. The returned row
 * id ranges can be pushed down to manifest readers and file readers to enable efficient random
 * access.
 *
 * <p>Note that there is a significant distinction between returning {@code null} and returning an
 * empty list:
 *
 * <ul>
 *   <li>{@code null} indicates that the predicate cannot be converted into a random-access pattern,
 *       meaning the filter is not consumable by this visitor.
 *   <li>An empty list indicates that no rows satisfy the predicate (e.g. {@code WHERE _ROW_ID = 3
 *       AND _ROW_ID IN (1, 2)}).
 * </ul>
 */
public class RowIdPredicateVisitor implements PredicateVisitor<Optional<List<Range>>> {

    @Override
    public Optional<List<Range>> visit(LeafPredicate predicate) {
        if (ROW_ID.name().equals(predicate.fieldName())) {
            LeafFunction function = predicate.function();
            if (function instanceof Equal || function instanceof In) {
                ArrayList<Long> rowIds = new ArrayList<>();
                for (Object literal : predicate.literals()) {
                    rowIds.add((Long) literal);
                }
                // The list output by getRangesFromList is already sorted,
                // and has no overlap
                return Optional.of(Range.toRanges(rowIds));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<List<Range>> visit(CompoundPredicate predicate) {
        CompoundPredicate.Function function = predicate.function();
        Optional<List<Range>> rowIds = Optional.empty();
        // `And` means we should get the intersection of all children.
        if (function instanceof And) {
            for (Predicate child : predicate.children()) {
                Optional<List<Range>> childList = child.visit(this);
                if (!childList.isPresent()) {
                    continue;
                }

                rowIds =
                        rowIds.map(ranges -> Optional.of(Range.and(ranges, childList.get())))
                                .orElse(childList);

                // shortcut for intersection
                if (rowIds.get().isEmpty()) {
                    return rowIds;
                }
            }
        } else if (function instanceof Or) {
            // `Or` means we should get the union of all children
            rowIds = Optional.of(new ArrayList<>());
            for (Predicate child : predicate.children()) {
                Optional<List<Range>> childList = child.visit(this);
                if (!childList.isPresent()) {
                    return Optional.empty();
                }

                rowIds.get().addAll(childList.get());
                rowIds = Optional.of(Range.sortAndMergeOverlap(rowIds.get(), true));
            }
        } else {
            // unexpected function type, just return empty
            return Optional.empty();
        }
        return rowIds;
    }

    @Override
    public Optional<List<Range>> visit(TransformPredicate predicate) {
        // do not support transform predicate now.
        return Optional.empty();
    }
}
