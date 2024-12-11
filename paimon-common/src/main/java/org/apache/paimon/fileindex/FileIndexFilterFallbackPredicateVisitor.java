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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Visit the predicate and extract the file index fallback predicate. */
public class FileIndexFilterFallbackPredicateVisitor
        implements PredicateVisitor<Optional<Predicate>> {

    private final Set<FieldRef> fields;

    public FileIndexFilterFallbackPredicateVisitor(Set<FieldRef> fields) {
        this.fields = fields;
    }

    @Override
    public Optional<Predicate> visit(LeafPredicate predicate) {
        if (fields.contains(predicate.fieldRef())) {
            return Optional.empty();
        }
        return Optional.of(predicate);
    }

    @Override
    public Optional<Predicate> visit(CompoundPredicate predicate) {
        List<Predicate> converted = new ArrayList<>();
        for (Predicate child : predicate.children()) {
            child.visit(this).ifPresent(converted::add);
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
