/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store;

import org.apache.flink.table.store.file.predicate.Literal;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Converts {@link SearchArgument} to {@link Predicate} with best effort. */
public class SearchArgumentToPredicateConverter {

    private static final Logger LOG =
            LoggerFactory.getLogger(SearchArgumentToPredicateConverter.class);

    private final ExpressionTree root;
    private final List<PredicateLeaf> leaves;
    private final List<String> columnNames;
    private final List<LogicalType> columnTypes;

    public SearchArgumentToPredicateConverter(
            SearchArgument sarg, List<String> columnNames, List<LogicalType> columnTypes) {
        this.root = sarg.getExpression();
        this.leaves = sarg.getLeaves();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public Optional<Predicate> convert() {
        try {
            return Optional.of(convertTree(root));
        } catch (UnsupportedOperationException e) {
            LOG.warn(
                    "Failed to convert predicate due to unsupported feature. "
                            + "Filter will be processed by Hive instead.",
                    e);
            return Optional.empty();
        }
    }

    private Predicate convertTree(ExpressionTree tree) {
        List<ExpressionTree> children = tree.getChildren();
        switch (tree.getOperator()) {
            case OR:
                return PredicateBuilder.or(
                        children.stream().map(this::convertTree).collect(Collectors.toList()));
            case AND:
                return PredicateBuilder.and(
                        children.stream().map(this::convertTree).collect(Collectors.toList()));
            case NOT:
                return convertTree(children.get(0))
                        .negate()
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                "Unsupported negate of "
                                                        + children.get(0).getOperator().name()));
            case LEAF:
                return convertLeaf(leaves.get(tree.getLeaf()));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + tree.getOperator().name());
        }
    }

    private Predicate convertLeaf(PredicateLeaf leaf) {
        String columnName = leaf.getColumnName();
        int idx = columnNames.indexOf(columnName);
        Preconditions.checkArgument(idx >= 0, "Column " + columnName + " not found.");
        LogicalType columnType = columnTypes.get(idx);
        switch (leaf.getOperator()) {
            case EQUALS:
                return PredicateBuilder.equal(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN:
                return PredicateBuilder.lessThan(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN_EQUALS:
                return PredicateBuilder.lessOrEqual(idx, toLiteral(columnType, leaf.getLiteral()));
            case IN:
                return PredicateBuilder.in(
                        idx,
                        leaf.getLiteralList().stream()
                                .map(o -> toLiteral(columnType, o))
                                .collect(Collectors.toList()));
            case BETWEEN:
                List<Object> literalList = leaf.getLiteralList();
                return PredicateBuilder.between(
                        idx,
                        toLiteral(columnType, literalList.get(0)),
                        toLiteral(columnType, literalList.get(1)));
            case IS_NULL:
                return PredicateBuilder.isNull(idx);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + leaf.getOperator());
        }
    }

    private Literal toLiteral(LogicalType literalType, Object o) {
        if (o instanceof HiveDecimalWritable) {
            o = ((HiveDecimalWritable) o).getHiveDecimal().bigDecimalValue();
        }
        return Literal.fromJavaObject(literalType, o);
    }
}
