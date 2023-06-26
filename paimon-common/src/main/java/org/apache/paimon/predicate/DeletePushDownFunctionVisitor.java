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
import java.util.Optional;

/** DeletePushDownFunctionVisitor visit the predicate and check if it can be push down. */
public class DeletePushDownFunctionVisitor implements FunctionVisitor<Optional<Long>> {

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
    public Optional<Long> visitIsNotNull(FieldRef fieldRef) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitIsNull(FieldRef fieldRef) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitStartsWith(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitLessThan(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitNotEqual(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitEqual(FieldRef fieldRef, Object literal) {
        if (partitionKeys.contains(fieldRef.name())) {
            if (predicatesSize == 1) {
                return Optional.empty();
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Not support delete pushdown for field %s because partition predicate is mixed with others",
                                fieldRef.name()));
            }
        }

        if (!primaryKeys.contains(fieldRef.name())) {
            throw new IllegalArgumentException(
                    String.format("Not support delete pushdown for field %s.", fieldRef.name()));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Long> visitGreaterThan(FieldRef fieldRef, Object literal) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitIn(FieldRef fieldRef, List<Object> literals) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        throw new IllegalArgumentException(
                String.format("Not support delete pushdown for field %s.", fieldRef.name()));
    }

    @Override
    public Optional<Long> visitAnd(List<Optional<Long>> children) {
        throw new IllegalArgumentException(String.format("Not support delete pushdown."));
    }

    @Override
    public Optional<Long> visitOr(List<Optional<Long>> children) {
        if (children.size() != 2) {
            throw new RuntimeException("Illegal or children: " + children.size());
        }

        if (children.get(0).isPresent() || children.get(1).isPresent()) {
            throw new RuntimeException("Illegal children");
        }

        return Optional.empty();
    }
}
