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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** Matches a full conjunction of equalities in physical composite-key order. */
public final class CompositeBTreePredicate {

    private CompositeBTreePredicate() {}

    /** SQL equalities with NULL, or distinct values for the same key field, cannot match. */
    public static boolean isContradictory(List<DataField> fields, Predicate predicate) {
        List<LeafPredicate> matched = match(fields, predicate).get();
        for (int i = 0; i < fields.size(); i++) {
            Object value = matched.get(i).literals().get(0);
            if (value == null) {
                return true;
            }
            Comparator<Object> comparator =
                    KeySerializer.create(fields.get(i).type()).createComparator();
            for (Predicate child : PredicateBuilder.splitAnd(predicate)) {
                if (child instanceof LeafPredicate) {
                    LeafPredicate leaf = (LeafPredicate) child;
                    if (leaf.function() instanceof Equal
                            && leaf.fieldRefOptional().equals(matched.get(i).fieldRefOptional())) {
                        Object other = leaf.literals().get(0);
                        if (other == null || comparator.compare(value, other) != 0) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public static Optional<List<LeafPredicate>> match(List<DataField> fields, Predicate predicate) {
        List<Predicate> conjuncts = PredicateBuilder.splitAnd(predicate);
        List<LeafPredicate> matched = new ArrayList<>();
        for (DataField field : fields) {
            LeafPredicate equality = null;
            for (Predicate conjunct : conjuncts) {
                if (conjunct instanceof LeafPredicate) {
                    LeafPredicate leaf = (LeafPredicate) conjunct;
                    if (leaf.function() instanceof Equal
                            && leaf.fieldRefOptional().isPresent()
                            && field.name().equals(leaf.fieldRefOptional().get().name())
                            && field.type().equalsIgnoreNullable(leaf.type())) {
                        equality = leaf;
                        break;
                    }
                }
            }
            if (equality == null) {
                return Optional.empty();
            }
            matched.add(equality);
        }
        return Optional.of(matched);
    }
}
