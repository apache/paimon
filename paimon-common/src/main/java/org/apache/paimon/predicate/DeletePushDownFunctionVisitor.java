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

import java.util.List;

/** DeletePushDownFunctionVisitor visit the predicate and check if it can be push down. */
public class DeletePushDownFunctionVisitor implements FunctionVisitor<Boolean> {

    private final List<String> primaryKeys;

    private final List<String> partitionKeys;

    private final int predicatesSize;

    public DeletePushDownFunctionVisitor(
            List<String> primaryKeys, List<String> partitionKeys, int size) {
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.predicatesSize = size;
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
        if (partitionKeys.contains(fieldRef.name())) {
            if (predicatesSize == 1) {
                return true;
            } else {
                return false;
            }
        }

        if (!primaryKeys.contains(fieldRef.name())) {
            return false;
        }
        return true;
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
        if (children.size() != 2) {
            return false;
        }
        if (children.get(0) && children.get(1)) {
            return true;
        }
        return false;
    }

    @Override
    public Boolean visitOr(List<Boolean> children) {
        if (children.size() != 2) {
            return false;
        }

        if (children.get(0) || children.get(1)) {
            return true;
        }

        return false;
    }
}
