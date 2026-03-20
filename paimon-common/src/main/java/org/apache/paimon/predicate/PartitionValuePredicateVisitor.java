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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link PredicateReplaceVisitor} that evaluates partition predicates against a known partition
 * value, replacing them with {@link AlwaysTrue} or {@link AlwaysFalse}.
 *
 * <p>For leaf predicates that only reference partition fields, the predicate is evaluated against
 * the given partition row (with field indices remapped from table schema to partition schema). If
 * the evaluation returns true, the predicate is replaced with AlwaysTrue; otherwise with
 * AlwaysFalse.
 *
 * <p>For leaf predicates that reference any non-partition field, the predicate is kept as-is.
 *
 * <p>For compound predicates (AND/OR), children are recursively visited and the result is
 * simplified via {@link PredicateBuilder#and} / {@link PredicateBuilder#or}.
 */
public class PartitionValuePredicateVisitor implements PredicateReplaceVisitor {

    private final Set<String> partitionFields;

    /** Mapping from table field index to partition field index. -1 if not a partition field. */
    private final int[] tableToPartitionMapping;

    private final InternalRow partitionRow;

    public PartitionValuePredicateVisitor(
            RowType tableType, RowType partitionType, InternalRow partitionRow) {
        this.partitionRow = partitionRow;
        this.partitionFields = new HashSet<>(partitionType.getFieldNames());

        List<String> tableFieldNames = tableType.getFieldNames();
        List<String> partitionFieldNames = partitionType.getFieldNames();

        this.tableToPartitionMapping = new int[tableFieldNames.size()];
        for (int i = 0; i < tableFieldNames.size(); i++) {
            tableToPartitionMapping[i] = partitionFieldNames.indexOf(tableFieldNames.get(i));
        }
    }

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        Set<String> refFields = PredicateVisitor.collectFieldNames(predicate);
        if (!partitionFields.containsAll(refFields)) {
            return Optional.of(predicate);
        }

        // Remap field indices from table schema to partition schema
        List<Object> remappedInputs = new ArrayList<>();
        for (Object input : predicate.transform().inputs()) {
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                int partIdx = tableToPartitionMapping[ref.index()];
                remappedInputs.add(new FieldRef(partIdx, ref.name(), ref.type()));
            } else {
                remappedInputs.add(input);
            }
        }

        // Evaluate the remapped predicate against the known partition row
        LeafPredicate remapped = predicate.copyWithNewInputs(remappedInputs);
        boolean result = remapped.test(partitionRow);
        return Optional.of(result ? PredicateBuilder.alwaysTrue() : PredicateBuilder.alwaysFalse());
    }
}
