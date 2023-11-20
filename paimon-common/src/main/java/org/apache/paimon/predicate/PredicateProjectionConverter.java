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

/** A {@link PredicateVisitor} which converts {@link Predicate} with projection. */
public class PredicateProjectionConverter implements PredicateVisitor<Optional<Predicate>> {

    private final Map<Integer, Integer> reversed;

    public PredicateProjectionConverter(int[] projection) {
        this.reversed = new HashMap<>();
        for (int i = 0; i < projection.length; i++) {
            reversed.put(projection[i], i);
        }
    }

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        int index = predicate.index();
        Integer adjusted = reversed.get(index);
        if (adjusted == null) {
            return Optional.empty();
        }

        return Optional.of(predicate.copyWithNewIndex(adjusted));
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
                    return Optional.empty();
                }
            }
        }
        return Optional.of(new CompoundPredicate(predicate.function(), converted));
    }
}
