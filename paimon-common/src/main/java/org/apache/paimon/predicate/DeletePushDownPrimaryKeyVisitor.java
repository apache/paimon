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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DeletePushDownPrimaryKeyVisitor visit the predicate and check if it only contains primary keys
 * and can be push down.
 *
 * <p>We will check: 1. all the primary keys are in the predicate with equal operator. 2. if step1
 * is satisfied, the other fields in the predicate could be ignored (e.g. return true).
 */
public class DeletePushDownPrimaryKeyVisitor implements FunctionVisitor<Boolean> {

    private final List<String> primaryKeys;

    private Map<String, Boolean> hit;

    public DeletePushDownPrimaryKeyVisitor(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        this.hit = new HashMap<>();
    }

    public boolean isHitAll() {
        return hit.size() == primaryKeys.size();
    }

    @Override
    public Boolean visitIsNotNull(FieldRef fieldRef) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitIsNull(FieldRef fieldRef) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitEqual(FieldRef fieldRef, Object literal) {
        if (primaryKeys.contains(fieldRef.name())) {
            hit.put(fieldRef.name(), true);
        }
        return true;
    }

    @Override
    public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return !primaryKeys.contains(fieldRef.name());
    }

    @Override
    public Boolean visitAnd(List<Boolean> children) {
        return children.stream().reduce((x, y) -> x && y).get();
    }

    @Override
    public Boolean visitOr(List<Boolean> children) {
        return children.stream().reduce((x, y) -> x || y).get();
    }
}
