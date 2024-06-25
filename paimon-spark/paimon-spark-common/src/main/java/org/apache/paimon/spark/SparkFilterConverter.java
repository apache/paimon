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

package org.apache.paimon.spark;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringStartsWith;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.convertJavaObject;

/** Conversion from {@link Filter} to {@link Predicate}. */
public class SparkFilterConverter {

    public static final List<String> SUPPORT_FILTERS =
            Arrays.asList(
                    "EqualTo",
                    "EqualNullSafe",
                    "GreaterThan",
                    "GreaterThanOrEqual",
                    "LessThan",
                    "LessThanOrEqual",
                    "In",
                    "IsNull",
                    "IsNotNull",
                    "And",
                    "Or",
                    "Not",
                    "StringStartsWith");

    private final RowType rowType;
    private final PredicateBuilder builder;

    public SparkFilterConverter(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    public Predicate convertIgnoreFailure(Filter filter) {
        try {
            return convert(filter);
        } catch (Exception e) {
            return null;
        }
    }

    public Predicate convert(Filter filter) {
        if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            // TODO deal with isNaN
            int index = fieldIndex(eq.attribute());
            Object literal = convertLiteral(index, eq.value());
            return builder.equal(index, literal);
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe eq = (EqualNullSafe) filter;
            if (eq.value() == null) {
                return builder.isNull(fieldIndex(eq.attribute()));
            } else {
                int index = fieldIndex(eq.attribute());
                Object literal = convertLiteral(index, eq.value());
                return builder.equal(index, literal);
            }
        } else if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            int index = fieldIndex(gt.attribute());
            Object literal = convertLiteral(index, gt.value());
            return builder.greaterThan(index, literal);
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gt = (GreaterThanOrEqual) filter;
            int index = fieldIndex(gt.attribute());
            Object literal = convertLiteral(index, gt.value());
            return builder.greaterOrEqual(index, literal);
        } else if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            int index = fieldIndex(lt.attribute());
            Object literal = convertLiteral(index, lt.value());
            return builder.lessThan(index, literal);
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lt = (LessThanOrEqual) filter;
            int index = fieldIndex(lt.attribute());
            Object literal = convertLiteral(index, lt.value());
            return builder.lessOrEqual(index, literal);
        } else if (filter instanceof In) {
            In in = (In) filter;
            int index = fieldIndex(in.attribute());
            return builder.in(
                    index,
                    Arrays.stream(in.values())
                            .map(v -> convertLiteral(index, v))
                            .collect(Collectors.toList()));
        } else if (filter instanceof IsNull) {
            return builder.isNull(fieldIndex(((IsNull) filter).attribute()));
        } else if (filter instanceof IsNotNull) {
            return builder.isNotNull(fieldIndex(((IsNotNull) filter).attribute()));
        } else if (filter instanceof And) {
            And and = (And) filter;
            return PredicateBuilder.and(convert(and.left()), convert(and.right()));
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            return PredicateBuilder.or(convert(or.left()), convert(or.right()));
        } else if (filter instanceof Not) {
            Not not = (Not) filter;
            return convert(not.child()).negate().orElseThrow(UnsupportedOperationException::new);
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith startsWith = (StringStartsWith) filter;
            int index = fieldIndex(startsWith.attribute());
            Object literal = convertLiteral(index, startsWith.value());
            return builder.startsWith(index, literal);
        }

        // TODO: AlwaysTrue, AlwaysFalse
        throw new UnsupportedOperationException(
                filter + " is unsupported. Support Filters: " + SUPPORT_FILTERS);
    }

    public Object convertLiteral(String field, Object value) {
        return convertLiteral(fieldIndex(field), value);
    }

    private int fieldIndex(String field) {
        int index = rowType.getFieldIndex(field);
        // TODO: support nested field
        if (index == -1) {
            throw new UnsupportedOperationException(
                    String.format("Nested field '%s' is unsupported.", field));
        }
        return index;
    }

    private Object convertLiteral(int index, Object value) {
        DataType type = rowType.getTypeAt(index);
        return convertJavaObject(type, value);
    }
}
