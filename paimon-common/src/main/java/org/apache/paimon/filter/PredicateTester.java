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

package org.apache.paimon.filter;

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;

import java.util.List;

/** Predicate test. */
public class PredicateTester implements PredicateVisitor<Boolean> {

    private final String columnName;
    private final PredicateFunctionChecker predicateFunctionChecker;

    public PredicateTester(String columnName, FilterInterface filterInterface) {
        this.columnName = columnName;
        this.predicateFunctionChecker = new PredicateFunctionChecker(filterInterface);
    }

    @Override
    public Boolean visit(LeafPredicate predicate) {
        if (columnName.equals(predicate.fieldName())) {
            return predicate
                    .function()
                    .visit(
                            predicateFunctionChecker,
                            new FieldRef(
                                    predicate.index(), predicate.fieldName(), predicate.type()),
                            predicate.literals());
        }
        return true;
    }

    @Override
    public Boolean visit(CompoundPredicate predicate) {

        if (predicate.function() instanceof Or) {
            for (Predicate predicate1 : predicate.children()) {
                if (predicate1.visit(this)) {
                    return true;
                }
            }
            return false;

        } else {
            for (Predicate predicate1 : predicate.children()) {
                if (!predicate1.visit(this)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class PredicateFunctionChecker implements FunctionVisitor<Boolean> {

        private final FilterInterface filterInterface;

        public PredicateFunctionChecker(FilterInterface filterInterface) {
            this.filterInterface = filterInterface;
        }

        @Override
        public Boolean visitIsNotNull(FieldRef fieldRef) {
            return filterInterface.testNotContains(null);
        }

        @Override
        public Boolean visitIsNull(FieldRef fieldRef) {
            return filterInterface.testContains(null);
        }

        @Override
        public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
            return filterInterface.testStartsWith(literal);
        }

        @Override
        public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
            return filterInterface.testLessThan(literal);
        }

        @Override
        public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return filterInterface.testGreaterOrEqual(literal);
        }

        @Override
        public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
            return filterInterface.testNotContains(literal);
        }

        @Override
        public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return filterInterface.testLessOrEqual(literal);
        }

        @Override
        public Boolean visitEqual(FieldRef fieldRef, Object literal) {
            return filterInterface.testContains(literal);
        }

        @Override
        public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
            return filterInterface.testGreaterThan(literal);
        }

        @Override
        public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
            Object[] keys = new byte[literals.size()][];
            for (int i = 0; i < literals.size(); i++) {
                keys[i] = literals.get(i);
            }
            return filterInterface.testIn(keys);
        }

        @Override
        public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
            Object[] keys = new byte[literals.size()][];
            for (int i = 0; i < literals.size(); i++) {
                keys[i] = literals.get(i);
            }
            return filterInterface.testNotIn(keys);
        }

        @Override
        public Boolean visitAnd(List<Boolean> children) {
            return true;
        }

        @Override
        public Boolean visitOr(List<Boolean> children) {
            return true;
        }
    }
}
