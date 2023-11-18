/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink;

import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.HashSet;
import java.util.Set;

/** Expression visitor which is for searching filters only related to partitions. */
public class PartitionExpressionVisitor implements ExpressionVisitor<Void> {

    private static final Set<FunctionDefinition> maybeSupportedFunction =
            ImmutableSet.of(
                    BuiltInFunctionDefinitions.AND,
                    BuiltInFunctionDefinitions.OR,
                    BuiltInFunctionDefinitions.EQUALS,
                    BuiltInFunctionDefinitions.NOT_EQUALS,
                    BuiltInFunctionDefinitions.GREATER_THAN,
                    BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                    BuiltInFunctionDefinitions.LESS_THAN,
                    BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                    BuiltInFunctionDefinitions.IN,
                    BuiltInFunctionDefinitions.IS_NULL,
                    BuiltInFunctionDefinitions.IS_NOT_NULL,
                    BuiltInFunctionDefinitions.LIKE);

    private final Set<String> partitionKeys;
    private final Set<String> visitedPartitionKeys;

    public PartitionExpressionVisitor(Set<String> partitionKeys) {
        Preconditions.checkArgument(
                !partitionKeys.isEmpty(), "The partition index should not be empty.");
        this.partitionKeys = partitionKeys;
        this.visitedPartitionKeys = new HashSet<>();
    }

    public Set<String> getVisitedInputRef() {
        return visitedPartitionKeys;
    }

    @Override
    public Void visit(CallExpression callExpression) {
        FunctionDefinition definition = callExpression.getFunctionDefinition();
        if (!maybeSupportedFunction.contains(definition)) {
            throw new PredicateConverter.UnsupportedExpression();
        }
        callExpression.getChildren().forEach(c -> c.accept(this));

        return null;
    }

    @Override
    public Void visit(ValueLiteralExpression valueLiteralExpression) {
        return null;
    }

    @Override
    public Void visit(FieldReferenceExpression fieldReferenceExpression) {
        if (!partitionKeys.contains(fieldReferenceExpression.getName())) {
            throw new PredicateConverter.UnsupportedExpression();
        }
        visitedPartitionKeys.add(fieldReferenceExpression.getName());
        return null;
    }

    @Override
    public Void visit(TypeLiteralExpression typeLiteralExpression) {
        throw new PredicateConverter.UnsupportedExpression();
    }

    @Override
    public Void visit(Expression expression) {
        throw new PredicateConverter.UnsupportedExpression();
    }
}
