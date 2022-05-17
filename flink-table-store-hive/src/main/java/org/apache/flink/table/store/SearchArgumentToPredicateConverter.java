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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.file.predicate.And;
import org.apache.flink.table.store.file.predicate.Equal;
import org.apache.flink.table.store.file.predicate.GreaterOrEqual;
import org.apache.flink.table.store.file.predicate.GreaterThan;
import org.apache.flink.table.store.file.predicate.IsNotNull;
import org.apache.flink.table.store.file.predicate.IsNull;
import org.apache.flink.table.store.file.predicate.LessOrEqual;
import org.apache.flink.table.store.file.predicate.LessThan;
import org.apache.flink.table.store.file.predicate.Literal;
import org.apache.flink.table.store.file.predicate.NotEqual;
import org.apache.flink.table.store.file.predicate.Or;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

/** Converts {@link SearchArgument} to {@link Predicate} with best effort. */
public class SearchArgumentToPredicateConverter {

    private static final Logger LOG =
            LoggerFactory.getLogger(SearchArgumentToPredicateConverter.class);

    private final ExpressionTree tree;
    private final List<PredicateLeaf> leaves;
    private final List<String> columnNames;
    private final List<LogicalType> columnTypes;

    public SearchArgumentToPredicateConverter(
            SearchArgument sarg, List<String> columnNames, List<LogicalType> columnTypes) {
        this.tree = sarg.getExpression();
        this.leaves = sarg.getLeaves();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public Optional<Predicate> convert() {
        try {
            return Optional.of(convertTree(tree));
        } catch (Throwable t) {
            LOG.warn("Failed to convert predicate. Filter will be processed by Hive instead.", t);
            return Optional.empty();
        }
    }

    private Predicate convertTree(ExpressionTree tree) {
        List<ExpressionTree> children = tree.getChildren();
        switch (tree.getOperator()) {
            case OR:
                return children.stream().map(this::convertTree).reduce(Or::new).get();
            case AND:
                return children.stream().map(this::convertTree).reduce(And::new).get();
            case NOT:
                return convertNotTree(children.get(0));
            case LEAF:
                return convertLeaf(leaves.get(tree.getLeaf()));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + tree.getOperator());
        }
    }

    private Predicate convertNotTree(ExpressionTree tree) {
        List<ExpressionTree> children = tree.getChildren();
        switch (tree.getOperator()) {
            case OR:
                return children.stream().map(this::convertNotTree).reduce(And::new).get();
            case AND:
                return children.stream().map(this::convertNotTree).reduce(Or::new).get();
            case NOT:
                return convertTree(children.get(0));
            case LEAF:
                return convertNotLeaf(leaves.get(tree.getLeaf()));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + tree.getOperator());
        }
    }

    private Predicate convertLeaf(PredicateLeaf leaf) {
        String columnName = leaf.getColumnName();
        int idx = columnNames.indexOf(columnName);
        Preconditions.checkArgument(idx >= 0, "Column " + columnName + " not found.");
        LogicalType columnType = columnTypes.get(idx);
        switch (leaf.getOperator()) {
            case EQUALS:
                return new Equal(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN:
                return new LessThan(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN_EQUALS:
                return new LessOrEqual(idx, toLiteral(columnType, leaf.getLiteral()));
            case IN:
                return leaf.getLiteralList().stream()
                        .map(o -> (Predicate) new Equal(idx, toLiteral(columnType, o)))
                        .reduce(Or::new)
                        .get();
            case BETWEEN:
                List<Object> literalList = leaf.getLiteralList();
                return new And(
                        new GreaterOrEqual(idx, toLiteral(columnType, literalList.get(0))),
                        new LessOrEqual(idx, toLiteral(columnType, literalList.get(1))));
            case IS_NULL:
                return new IsNull(idx);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + tree.getOperator());
        }
    }

    private Predicate convertNotLeaf(PredicateLeaf leaf) {
        String columnName = leaf.getColumnName();
        int idx = columnNames.indexOf(columnName);
        Preconditions.checkArgument(idx >= 0, "Column " + columnName + " not found.");
        LogicalType columnType = columnTypes.get(idx);
        switch (leaf.getOperator()) {
            case EQUALS:
                return new NotEqual(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN:
                return new GreaterOrEqual(idx, toLiteral(columnType, leaf.getLiteral()));
            case LESS_THAN_EQUALS:
                return new GreaterThan(idx, toLiteral(columnType, leaf.getLiteral()));
            case IN:
                return leaf.getLiteralList().stream()
                        .map(o -> (Predicate) new NotEqual(idx, toLiteral(columnType, o)))
                        .reduce(And::new)
                        .get();
            case BETWEEN:
                List<Object> literalList = leaf.getLiteralList();
                return new Or(
                        new LessThan(idx, toLiteral(columnType, literalList.get(0))),
                        new GreaterThan(idx, toLiteral(columnType, literalList.get(1))));
            case IS_NULL:
                return new IsNotNull(idx);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator " + tree.getOperator());
        }
    }

    private Literal toLiteral(LogicalType literalType, Object o) {
        if (o == null) {
            throw new UnsupportedOperationException("Null literals are currently unsupported");
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
            case BIGINT:
            case DOUBLE:
                return new Literal(literalType, o);
            case TINYINT:
                return new Literal(literalType, ((Long) o).byteValue());
            case SMALLINT:
                return new Literal(literalType, ((Long) o).shortValue());
            case INTEGER:
                return new Literal(literalType, ((Long) o).intValue());
            case FLOAT:
                return new Literal(literalType, ((Double) o).floatValue());
            case VARCHAR:
                return new Literal(literalType, StringData.fromString(o.toString()));
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof Timestamp) {
                    localDate = ((Timestamp) o).toLocalDateTime().toLocalDate();
                } else if (o instanceof Date) {
                    localDate = ((Date) o).toLocalDate();
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected date literal of class " + o.getClass().getName());
                }
                LocalDate epochDay =
                        Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                int numberOfDays = (int) ChronoUnit.DAYS.between(epochDay, localDate);
                return new Literal(literalType, numberOfDays);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return new Literal(
                        literalType,
                        DecimalData.fromBigDecimal(
                                ((HiveDecimalWritable) o).getHiveDecimal().bigDecimalValue(),
                                precision,
                                scale));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new Literal(literalType, TimestampData.fromTimestamp((Timestamp) o));
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }
}
