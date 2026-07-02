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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.sources.AlwaysFalse;
import org.apache.spark.sql.sources.AlwaysTrue;
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
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.convertJavaObject;

/** Conversion from {@link Filter} to {@link Predicate}, remove it when Spark 3.2 is dropped. */
public class SparkFilterConverter {

    public static final List<String> SUPPORT_FILTERS =
            Arrays.asList(
                    "AlwaysTrue",
                    "AlwaysFalse",
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
                    "StringStartsWith",
                    "StringEndsWith",
                    "StringContains");

    private final RowType rowType;
    private final PredicateBuilder builder;

    public SparkFilterConverter(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
    }

    @Nullable
    public Predicate convertIgnoreFailure(Filter filter) {
        return convert(filter, true);
    }

    @Nullable
    public Predicate convert(Filter filter, boolean ignoreFailure) {
        try {
            return convert(filter);
        } catch (Exception e) {
            if (ignoreFailure) {
                return null;
            } else {
                throw e;
            }
        }
    }

    public Predicate convert(Filter filter) {
        if (filter instanceof AlwaysTrue) {
            return PredicateBuilder.alwaysTrue();
        } else if (filter instanceof AlwaysFalse) {
            return PredicateBuilder.alwaysFalse();
        } else if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            FieldInfo fieldInfo = resolveField(eq.attribute());
            if (isNaN(eq.value())) {
                return builder.isNaN(fieldInfo.transform());
            }
            Object literal = convertLiteral(fieldInfo.type(), eq.value());
            return builder.equal(fieldInfo.transform(), literal);
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe eq = (EqualNullSafe) filter;
            FieldInfo fieldInfo = resolveField(eq.attribute());
            if (eq.value() == null) {
                return builder.isNull(fieldInfo.transform());
            } else {
                Object literal = convertLiteral(fieldInfo.type(), eq.value());
                return builder.equal(fieldInfo.transform(), literal);
            }
        } else if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            FieldInfo fieldInfo = resolveField(gt.attribute());
            Object literal = convertLiteral(fieldInfo.type(), gt.value());
            return builder.greaterThan(fieldInfo.transform(), literal);
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gt = (GreaterThanOrEqual) filter;
            FieldInfo fieldInfo = resolveField(gt.attribute());
            Object literal = convertLiteral(fieldInfo.type(), gt.value());
            return builder.greaterOrEqual(fieldInfo.transform(), literal);
        } else if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            FieldInfo fieldInfo = resolveField(lt.attribute());
            Object literal = convertLiteral(fieldInfo.type(), lt.value());
            return builder.lessThan(fieldInfo.transform(), literal);
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lt = (LessThanOrEqual) filter;
            FieldInfo fieldInfo = resolveField(lt.attribute());
            Object literal = convertLiteral(fieldInfo.type(), lt.value());
            return builder.lessOrEqual(fieldInfo.transform(), literal);
        } else if (filter instanceof In) {
            In in = (In) filter;
            FieldInfo fieldInfo = resolveField(in.attribute());
            return builder.in(
                    fieldInfo.transform(),
                    Arrays.stream(in.values())
                            .map(v -> convertLiteral(fieldInfo.type(), v))
                            .collect(Collectors.toList()));
        } else if (filter instanceof IsNull) {
            FieldInfo fieldInfo = resolveField(((IsNull) filter).attribute());
            return builder.isNull(fieldInfo.transform());
        } else if (filter instanceof IsNotNull) {
            FieldInfo fieldInfo = resolveField(((IsNotNull) filter).attribute());
            return builder.isNotNull(fieldInfo.transform());
        } else if (filter instanceof And) {
            And and = (And) filter;
            return PredicateBuilder.and(convert(and.left()), convert(and.right()));
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            return PredicateBuilder.or(convert(or.left()), convert(or.right()));
        } else if (filter instanceof Not) {
            Not not = (Not) filter;
            Optional<Predicate> negate = convert(not.child()).negate();
            if (negate.isPresent()) {
                return negate.get();
            }
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith startsWith = (StringStartsWith) filter;
            FieldInfo fieldInfo = resolveField(startsWith.attribute());
            Object literal = convertLiteral(fieldInfo.type(), startsWith.value());
            return builder.startsWith(fieldInfo.transform(), literal);
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith endsWith = (StringEndsWith) filter;
            FieldInfo fieldInfo = resolveField(endsWith.attribute());
            Object literal = convertLiteral(fieldInfo.type(), endsWith.value());
            return builder.endsWith(fieldInfo.transform(), literal);
        } else if (filter instanceof StringContains) {
            StringContains contains = (StringContains) filter;
            FieldInfo fieldInfo = resolveField(contains.attribute());
            Object literal = convertLiteral(fieldInfo.type(), contains.value());
            return builder.contains(fieldInfo.transform(), literal);
        }

        throw new UnsupportedOperationException(
                filter + " is unsupported. Support Filters: " + SUPPORT_FILTERS);
    }

    private static boolean isNaN(Object value) {
        if (value instanceof Float) {
            return Float.isNaN((Float) value);
        }
        if (value instanceof Double) {
            return Double.isNaN((Double) value);
        }
        return false;
    }

    public Object convertLiteral(String field, Object value) {
        FieldInfo fieldInfo = resolveField(field);
        return convertLiteral(fieldInfo.type(), value);
    }

    public String convertString(String field, Object value) {
        Object literal = convertLiteral(field, value);
        return literal == null ? null : literal.toString();
    }

    private static class FieldInfo {
        private final Transform transform;
        private final DataType type;

        public FieldInfo(Transform transform, DataType type) {
            this.transform = transform;
            this.type = type;
        }

        public Transform transform() {
            return transform;
        }

        public DataType type() {
            return type;
        }
    }

    private FieldInfo resolveField(String field) {
        // Paimon schema does not forbid top-level column names containing '.', so always try an
        // exact top-level lookup first and only fall back to nested path resolution when no such
        // field exists.
        int exactIndex = rowType.getFieldIndex(field);
        if (exactIndex != -1) {
            DataType fieldType = rowType.getTypeAt(exactIndex);
            Transform transform = new FieldTransform(new FieldRef(exactIndex, field, fieldType));
            return new FieldInfo(transform, fieldType);
        }

        String[] parts = field.split("\\.");
        if (parts.length == 1) {
            throw new UnsupportedOperationException(
                    String.format("Field '%s' is not found in table schema.", field));
        }

        int topLevelIndex = rowType.getFieldIndex(parts[0]);
        if (topLevelIndex == -1) {
            throw new UnsupportedOperationException(
                    String.format("Field '%s' is not found in table schema.", field));
        }

        DataType fieldType = getNestedFieldType(rowType, parts);
        if (fieldType == null) {
            throw new UnsupportedOperationException(
                    String.format("Nested field '%s' is unsupported.", field));
        }
        int[] nestedIndexes = new int[parts.length - 1];
        int[] nestedArities = new int[parts.length - 1];
        DataType currentType = rowType.getTypeAt(topLevelIndex);
        for (int i = 0; i < parts.length - 1; i++) {
            RowType currentSelection = (RowType) currentType;
            nestedArities[i] = currentSelection.getFieldCount();
            String nextPart = parts[i + 1];
            int nextIndex = currentSelection.getFieldIndex(nextPart);
            nestedIndexes[i] = nextIndex;
            currentType = currentSelection.getTypeAt(nextIndex);
        }

        Transform transform =
                new FieldTransform(
                        new FieldRef(
                                topLevelIndex, field, fieldType, nestedIndexes, nestedArities));
        return new FieldInfo(transform, fieldType);
    }

    private DataType getNestedFieldType(RowType rowType, String[] path) {
        DataType currentType = rowType;
        for (String part : path) {
            if (currentType instanceof RowType) {
                RowType currentSelection = (RowType) currentType;
                int idx = currentSelection.getFieldIndex(part);
                if (idx == -1) {
                    return null;
                }
                currentType = currentSelection.getTypeAt(idx);
            } else {
                return null;
            }
        }
        return currentType;
    }

    private Object convertLiteral(DataType type, Object value) {
        return convertJavaObject(type, value);
    }
}
