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

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Visit the predicate and check if it only contains primary keys and can be push down.
 *
 * <p>We will check: all the primary keys are in the predicate with equal operator.
 *
 * <p>TODO: support IN.
 */
public class AllPrimaryKeyEqualVisitor implements FunctionVisitor<Set<String>> {

    private final List<String> primaryKeys;

    public AllPrimaryKeyEqualVisitor(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public Set<String> visitIsNotNull(FieldRef fieldRef) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitIsNull(FieldRef fieldRef) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitLessThan(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitEqual(FieldRef fieldRef, Object literal) {
        if (primaryKeys.contains(fieldRef.name())) {
            return Collections.singleton(fieldRef.name());
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public Set<String> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> visitAnd(List<Set<String>> children) {
        return children.stream().reduce(Sets::union).get();
    }

    @Override
    public Set<String> visitOr(List<Set<String>> children) {
        return Collections.emptySet();
    }
}
