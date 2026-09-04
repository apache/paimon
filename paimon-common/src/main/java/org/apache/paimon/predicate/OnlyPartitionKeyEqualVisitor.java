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
 * Visit the predicate and check if it only contains partition keys and can be push down.
 *
 * <p>TODO: more filters if {@code BatchWriteBuilder} supports predicate.
 */
public class OnlyPartitionKeyEqualVisitor implements FunctionVisitor<Boolean> {

    private final List<String> partitionKeys;

    private final Map<String, String> partitions;
    private boolean contradiction = false;

    public OnlyPartitionKeyEqualVisitor(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        partitions = new HashMap<>();
    }

    public Map<String, String> partitions() {
        return partitions;
    }

    @Override
    public Boolean visitIsNotNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitIsNull(FieldRef fieldRef) {
        return false;
    }

    @Override
    public Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitEndsWith(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitContains(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitArrayContains(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitArraysOverlap(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitArrayContainsAll(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitLike(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitEqual(FieldRef fieldRef, Object literal) {
        boolean contains = partitionKeys.contains(fieldRef.name());
        if (contains) {
            String value = literal.toString();
            String existing = partitions.put(fieldRef.name(), value);
            if (existing != null && !existing.equals(value)) {
                // Contradictory condition on the same key (pt = 'a' AND pt = 'b'):
                // no partition can match, so the delete must not drop anything.
                contradiction = true;
            }
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
        return false;
    }

    @Override
    public Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return false;
    }

    @Override
    public Boolean visitAnd(List<Boolean> children) {
        if (contradiction) {
            // Two equals on the same partition key carried different literals, so the
            // conjunction matches nothing; dropping the partition would delete rows
            // the predicate never selected.
            return false;
        }
        return children.stream().reduce((first, second) -> first && second).get();
    }

    @Override
    public Boolean visitOr(List<Boolean> children) {
        return false;
    }

    @Override
    public Boolean visitNonFieldLeaf(LeafPredicate predicate) {
        return false;
    }
}
