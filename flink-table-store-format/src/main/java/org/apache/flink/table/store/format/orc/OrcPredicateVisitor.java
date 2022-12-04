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

package org.apache.flink.table.store.format.orc;

import org.apache.flink.orc.OrcFilters;
import org.apache.flink.orc.OrcFilters.Predicate;
import org.apache.flink.table.store.file.predicate.CompoundPredicate;
import org.apache.flink.table.store.file.predicate.Or;
import org.apache.flink.table.store.file.predicate.PredicateVisitor;

import java.util.Optional;

/**
 * A {@link PredicateVisitor} to convert {@link
 * org.apache.flink.table.store.file.predicate.Predicate} to {@link Predicate}.
 */
public interface OrcPredicateVisitor extends PredicateVisitor<Optional<Predicate>> {
    @Override
    default Optional<Predicate> visit(CompoundPredicate predicate) {
        if (predicate.function().equals(Or.INSTANCE)) {
            if (predicate.children().size() != 2) {
                throw new RuntimeException("Illegal or children: " + predicate.children().size());
            }

            Optional<Predicate> c1 = predicate.children().get(0).visit(this);
            if (!c1.isPresent()) {
                return Optional.empty();
            }
            Optional<Predicate> c2 = predicate.children().get(1).visit(this);
            return c2.map(value -> new OrcFilters.Or(c1.get(), value));
        }
        return Optional.empty();
    }
}
