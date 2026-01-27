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

package org.apache.paimon.table.format.predicate;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;

/** Utility methods for working with {@link Predicate}s. */
public class PredicateUtils {

    // Use splitAnd split the predicate, then group them by the partition fields.
    public static Map<String, Predicate> splitPartitionPredicate(
            RowType partitionType, Predicate predicate) {
        int[] fieldMap = new int[partitionType.getFields().size()];
        Arrays.fill(fieldMap, 0);
        List<Predicate> predicates = PredicateBuilder.splitAnd(predicate);
        Set<String> partitionFieldNames = new HashSet<>(partitionType.getFieldNames());
        Map<String, Predicate> result = new HashMap<>();

        for (Predicate sub : predicates) {
            // Collect all field names referenced by this predicate
            Set<String> referencedFields = sub.visit(new FieldNameCollector());
            Optional<Predicate> transformed = transformFieldMapping(sub, fieldMap);
            if (transformed.isPresent() && referencedFields.size() == 1) {
                Predicate child = transformed.get();
                // Only include predicates that reference exactly one partition field
                String fieldName = referencedFields.iterator().next();
                if (partitionFieldNames.contains(fieldName)) {
                    if (result.containsKey(fieldName)) {
                        // Combine with existing predicate using AND
                        result.put(fieldName, PredicateBuilder.and(result.get(fieldName), child));
                    } else {
                        result.put(fieldName, child);
                    }
                }
            }
        }

        return result;
    }

    /** A visitor that collects all field names referenced by a predicate. */
    private static class FieldNameCollector implements PredicateVisitor<Set<String>> {

        @Override
        public Set<String> visit(LeafPredicate predicate) {
            Set<String> fieldNames = new HashSet<>();
            for (Object input : predicate.transform().inputs()) {
                if (input instanceof FieldRef) {
                    fieldNames.add(((FieldRef) input).name());
                }
            }
            return fieldNames;
        }

        @Override
        public Set<String> visit(CompoundPredicate predicate) {
            Set<String> fieldNames = new HashSet<>();
            for (Predicate child : predicate.children()) {
                fieldNames.addAll(child.visit(this));
            }
            return fieldNames;
        }
    }
}
