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

package org.apache.paimon.hive;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.convertJavaObject;

/** Converts {@link SearchArgument} to {@link Predicate} with the best effort. */
public class SearchArgumentToPredicateConverter {

    private static final Logger LOG =
            LoggerFactory.getLogger(SearchArgumentToPredicateConverter.class);

    private final ExpressionTree root;
    private final List<PredicateLeaf> leaves;
    private final List<String> hiveColumnNames;
    private final List<DataType> columnTypes;
    @Nullable private final Set<String> readColumnNames;
    private final PredicateBuilder builder;

    public SearchArgumentToPredicateConverter(
            SearchArgument searchArgument,
            List<String> columnNames,
            List<DataType> columnTypes,
            @Nullable Set<String> readColumnNames) {
        this.root = searchArgument.getExpression();
        this.leaves = searchArgument.getLeaves();
        this.hiveColumnNames =
                columnNames.stream().map(String::toLowerCase).collect(Collectors.toList());
        this.columnTypes = columnTypes;
        if (readColumnNames != null) {
            readColumnNames =
                    readColumnNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
        }
        this.readColumnNames = readColumnNames;

        this.builder =
                new PredicateBuilder(
                        RowType.of(
                                this.columnTypes.toArray(new DataType[0]),
                                columnNames.toArray(new String[0])));
    }

    public Optional<Predicate> convert() {
        List<ExpressionTree> trees = new ArrayList<>();
        if (root.getOperator() == ExpressionTree.Operator.AND) {
            trees.addAll(root.getChildren());
        } else {
            trees.add(root);
        }

        List<Predicate> converted = new ArrayList<>();
        for (ExpressionTree tree : trees) {
            try {
                converted.add(convertTree(tree));
            } catch (UnsupportedOperationException e) {
                LOG.warn(
                        "Failed to convert predicate "
                                + tree
                                + "  due to unsupported feature. "
                                + "This part of filter will be processed by Hive instead.",
                        e);
            }
        }
        return converted.isEmpty()
                ? Optional.empty()
                : Optional.of(PredicateBuilder.and(converted));
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
        if (readColumnNames != null && !readColumnNames.contains(columnName)) {
            throw new UnsupportedOperationException(
                    "Column "
                            + columnName
                            + " is not needed when reading. "
                            + "It is very likely that the Hive table you're reading is a partitioned table and "
                            + columnName
                            + " is a partition column.");
        }

        int idx = hiveColumnNames.indexOf(columnName);
        Preconditions.checkArgument(idx >= 0, "Column " + columnName + " not found.");
        DataType columnType = columnTypes.get(idx);
        switch (leaf.getOperator()) {
            case EQUALS:
                return builder.equal(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN:
                return builder.lessThan(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN_EQUALS:
                return builder.lessOrEqual(idx, toLiteral(columnType, leaf.getLiteral()));
            case IN:
                return builder.in(
                        idx,
                        leaf.getLiteralList().stream()
                                .map(o -> toLiteral(columnType, o))
                                .collect(Collectors.toList()));
            case BETWEEN:
                List<Object> literalList = leaf.getLiteralList();
                return builder.between(
                        idx,
                        toLiteral(columnType, literalList.get(0)),
                        toLiteral(columnType, literalList.get(1)));
            case IS_NULL:
                return builder.isNull(idx);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + leaf.getOperator());
        }
    }

    private Object toLiteral(DataType literalType, Object o) {
        if (o instanceof HiveDecimalWritable) {
            o = ((HiveDecimalWritable) o).getHiveDecimal().bigDecimalValue();
        }
        return convertJavaObject(literalType, o);
    }
}
