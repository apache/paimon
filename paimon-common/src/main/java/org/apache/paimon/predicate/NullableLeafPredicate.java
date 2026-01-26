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

import java.util.List;
import java.util.stream.Collectors;

/** A {@link LeafPredicate} that handles nullable values. */
public class NullableLeafPredicate extends LeafPredicate {

    public NullableLeafPredicate(
            Transform transform, LeafFunction function, List<Object> literals) {
        super(transform, function, literals);
    }

    @Override
    public boolean test(InternalRow row) {
        Object value = transform.transform(row);
        if (value == null) {
            return true;
        }
        return function.test(transform.outputType(), value, literals);
    }

    public static NullableLeafPredicate fromLeafPredicate(LeafPredicate leafPredicate) {
        return new NullableLeafPredicate(
                leafPredicate.transform(), leafPredicate.function(), leafPredicate.literals());
    }

    public static Predicate from(Predicate predicate) {
        return predicate.visit(Visitor.INSTANCE);
    }

    private static class Visitor implements PredicateVisitor<Predicate> {

        private static final Visitor INSTANCE = new Visitor();

        @Override
        public Predicate visit(LeafPredicate predicate) {
            return fromLeafPredicate(predicate);
        }

        @Override
        public Predicate visit(CompoundPredicate predicate) {
            CompoundPredicate.Function function = predicate.function();
            List<Predicate> newChildren =
                    predicate.children().stream()
                            .map(child -> child.visit(this))
                            .collect(Collectors.toList());
            return new CompoundPredicate(function, newChildren);
        }
    }
}
