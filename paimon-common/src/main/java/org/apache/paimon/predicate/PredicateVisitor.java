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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** A visitor to visit {@link Predicate}. */
public interface PredicateVisitor<T> {

    T visit(LeafPredicate predicate);

    T visit(CompoundPredicate predicate);

    static Set<String> collectFieldNames(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptySet();
        }
        return predicate.visit(new FieldNameCollector());
    }

    /** A visitor that collects all field names referenced by a predicate. */
    class FieldNameCollector implements PredicateVisitor<Set<String>> {

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
