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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.BitLengthTransform;
import org.apache.paimon.predicate.CastTransform;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.DateAddTransform;
import org.apache.paimon.predicate.DateDiffTransform;
import org.apache.paimon.predicate.DateTruncTransform;
import org.apache.paimon.predicate.DayOfWeekTransform;
import org.apache.paimon.predicate.DayOfYearTransform;
import org.apache.paimon.predicate.DayTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.HourTransform;
import org.apache.paimon.predicate.LengthTransform;
import org.apache.paimon.predicate.LowerTransform;
import org.apache.paimon.predicate.MinuteTransform;
import org.apache.paimon.predicate.MonthTransform;
import org.apache.paimon.predicate.OverlayTransform;
import org.apache.paimon.predicate.PadTransform;
import org.apache.paimon.predicate.QuarterTransform;
import org.apache.paimon.predicate.SecondTransform;
import org.apache.paimon.predicate.SubstringTransform;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TranslateTransform;
import org.apache.paimon.predicate.TrimTransform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.predicate.WeekTransform;
import org.apache.paimon.predicate.WeekdayTransform;
import org.apache.paimon.predicate.YearTransform;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compiles the supported SQL scalar expressions into existing storage transforms. */
final class GeneratedExpression {

    private final int fieldCount;
    private final List<Transform> steps;
    private final List<FieldRef> references;
    private final String normalized;

    private GeneratedExpression(Parser parser) {
        this.fieldCount = parser.rowType.getFieldCount();
        this.steps = parser.steps;
        this.references = new ArrayList<>(parser.references.values());
        this.normalized = parser.normalized.toString();
    }

    static GeneratedExpression parse(
            String expression, RowType rowType, Function<String, String> resolver) {
        Parser parser = new Parser(expression, rowType, resolver);
        Object root = parser.value();
        checkArgument(root instanceof FieldRef, "A generation expression must reference a column.");
        if (((FieldRef) root).index() < rowType.getFieldCount()) {
            parser.emit(new FieldTransform((FieldRef) root));
        }
        checkArgument(parser.token == null, "Unsupported generation expression: %s", expression);
        return new GeneratedExpression(parser);
    }

    DataType outputType() {
        return steps.get(steps.size() - 1).outputType();
    }

    List<FieldRef> references() {
        return references;
    }

    String normalized() {
        return normalized;
    }

    Object evaluate(InternalRow row) {
        if (steps.size() == 1) {
            return steps.get(0).transform(row);
        }
        // Nested calls use temporary field references, without changing the table schema.
        GenericRow inputs = new GenericRow(fieldCount + steps.size());
        for (FieldRef ref : references) {
            inputs.setField(ref.index(), InternalRowUtils.get(row, ref.index(), ref.type()));
        }
        for (int i = 0; i < steps.size(); i++) {
            inputs.setField(fieldCount + i, steps.get(i).transform(inputs));
        }
        return inputs.getField(fieldCount + steps.size() - 1);
    }

    private static final class Parser {

        private static final Pattern TOKEN =
                Pattern.compile(
                        "\\s*(`(?:``|[^`])+`|'(?:''|[^'])*'|[+-]?\\d+|[a-zA-Z_][a-zA-Z_0-9]*|[(),])");

        private final String expression;
        private final RowType rowType;
        private final Function<String, String> resolver;
        private final List<Transform> steps = new ArrayList<>();
        private final Map<String, FieldRef> references = new LinkedHashMap<>();
        private final StringBuilder normalized = new StringBuilder();
        private int position;
        private String token;
        private int tokenStart;

        private Parser(String expression, RowType rowType, Function<String, String> resolver) {
            this.expression = expression;
            this.rowType = rowType;
            this.resolver = resolver;
            next();
        }

        private void next() {
            Matcher matcher = TOKEN.matcher(expression);
            matcher.region(position, expression.length());
            if (matcher.lookingAt()) {
                tokenStart = matcher.start(1);
                normalized.append(expression, position, tokenStart);
                token = matcher.group(1);
                position = matcher.end();
            } else {
                checkArgument(
                        expression.substring(position).trim().isEmpty(),
                        "Unsupported generation expression near '%s'. Only top-level field references and supported scalar calls are allowed.",
                        expression.substring(position));
                normalized.append(expression.substring(position));
                position = expression.length();
                token = null;
            }
        }

        private String take() {
            String result = token;
            checkArgument(result != null, "Incomplete generation expression: %s", expression);
            normalized.append(result);
            next();
            return result;
        }

        private void expect(String expected) {
            checkArgument(
                    expected.equalsIgnoreCase(token),
                    "Expected '%s' in generation expression: %s",
                    expected,
                    expression);
            take();
        }

        private Object value() {
            checkArgument(token != null, "Incomplete generation expression: %s", expression);
            String name = token;
            if (name.startsWith("'")) {
                take();
                return BinaryString.fromString(
                        name.substring(1, name.length() - 1).replace("''", "'"));
            }
            if (name.matches("[+-]?\\d+")) {
                take();
                return Integer.valueOf(name);
            }
            if (name.equalsIgnoreCase("NULL")) {
                take();
                return null;
            }
            // An identifier may name a function, a date literal, or a top-level column.
            int start = tokenStart;
            int end = position;
            take();
            if ("(".equals(token) && !name.startsWith("`")) {
                take();
                if (name.equalsIgnoreCase("CAST")) {
                    Object source = value();
                    checkArgument(
                            source instanceof FieldRef, "CAST requires a column or scalar call.");
                    expect("AS");
                    StringBuilder type = new StringBuilder();
                    int depth = 0;
                    while (token != null && (depth > 0 || !token.equals(")"))) {
                        if (token.equals("(")) {
                            depth++;
                        }
                        if (token.equals(")")) {
                            depth--;
                        }
                        type.append(take()).append(' ');
                    }
                    expect(")");
                    DataType target =
                            DataTypeJsonParser.parseAtomicTypeSQLString(type.toString().trim());
                    checkArgument(
                            !target.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                            "Generated expressions cannot use TIMESTAMP WITH LOCAL TIME ZONE.");
                    return emit(new CastTransform((FieldRef) source, target));
                }
                List<Object> inputs = new ArrayList<>();
                if (!")".equals(token)) {
                    inputs.add(value());
                    while (",".equals(token)) {
                        take();
                        inputs.add(value());
                    }
                }
                expect(")");
                return emit(function(name.toUpperCase(Locale.ROOT), inputs));
            }
            if (name.equalsIgnoreCase("DATE") && token != null && token.startsWith("'")) {
                String date = take();
                return Math.toIntExact(
                        LocalDate.parse(date.substring(1, date.length() - 1)).toEpochDay());
            }
            String unquoted =
                    name.startsWith("`")
                            ? name.substring(1, name.length() - 1).replace("``", "`")
                            : name;
            String resolved = resolver.apply(unquoted);
            checkArgument(
                    rowType.containsField(resolved),
                    "Generation expression references unknown column '%s'.",
                    resolved);
            DataField field = rowType.getField(resolved);
            checkArgument(
                    field.generationExpression() == null,
                    "Generated columns cannot reference generated column '%s'.",
                    resolved);
            checkArgument(
                    !field.type().is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                    "Generated expressions cannot use TIMESTAMP WITH LOCAL TIME ZONE.");
            // next() may have appended whitespace following this identifier.
            if (!resolved.equals(unquoted)) {
                int normalizedEnd =
                        normalized.length()
                                - (token == null ? expression.length() - end : tokenStart - end);
                normalized.replace(
                        normalizedEnd - (end - start),
                        normalizedEnd,
                        EncodingUtils.escapeIdentifier(resolved));
            }
            FieldRef ref = new FieldRef(rowType.getFieldIndex(resolved), resolved, field.type());
            references.put(resolved, ref);
            return ref;
        }

        private FieldRef emit(Transform transform) {
            FieldRef result =
                    new FieldRef(
                            rowType.getFieldCount() + steps.size(), "", transform.outputType());
            steps.add(transform);
            return result;
        }

        private Transform function(String name, List<Object> inputs) {
            switch (name) {
                case "CONCAT":
                    return new ConcatTransform(inputs);
                case "CONCAT_WS":
                    return new ConcatWsTransform(inputs);
                case "UPPER":
                    return new UpperTransform(inputs);
                case "LOWER":
                    return new LowerTransform(inputs);
                case "SUBSTRING":
                case "SUBSTR":
                    Transform substring = new SubstringTransform(inputs);
                    Object source = inputs.get(0);
                    checkArgument(
                            !(source instanceof FieldRef)
                                    || ((FieldRef) source)
                                            .type()
                                            .is(DataTypeFamily.CHARACTER_STRING),
                            "SUBSTRING source must be a string.");
                    for (int i = 1; i < inputs.size(); i++) {
                        Object position = inputs.get(i);
                        checkArgument(
                                position == null
                                        || position instanceof Integer
                                        || (position instanceof FieldRef
                                                && ((FieldRef) position)
                                                        .type()
                                                        .is(DataTypeFamily.INTEGER_NUMERIC)),
                                "SUBSTRING positions must be integers.");
                    }
                    return substring;
                case "LENGTH":
                case "CHAR_LENGTH":
                    return new LengthTransform(inputs);
                case "BIT_LENGTH":
                    return new BitLengthTransform(inputs);
                case "TRANSLATE":
                    return new TranslateTransform(inputs);
                case "OVERLAY":
                    return new OverlayTransform(inputs);
                case "LPAD":
                    return new PadTransform(inputs, PadTransform.Direction.LEFT);
                case "RPAD":
                    return new PadTransform(inputs, PadTransform.Direction.RIGHT);
                case "TRIM":
                    return new TrimTransform(inputs, TrimTransform.Flag.BOTH);
                case "LTRIM":
                    return new TrimTransform(inputs, TrimTransform.Flag.LEADING);
                case "RTRIM":
                    return new TrimTransform(inputs, TrimTransform.Flag.TRAILING);
                case "DATE_ADD":
                    return new DateAddTransform(inputs);
                case "DATEDIFF":
                case "DATE_DIFF":
                    return new DateDiffTransform(inputs);
                case "TRUNC":
                    return new DateTruncTransform(inputs);
                default:
            }
            checkArgument(
                    inputs.size() == 1 && inputs.get(0) instanceof FieldRef,
                    "Unsupported generation function or arguments: %s",
                    name);
            FieldRef field = (FieldRef) inputs.get(0);
            switch (name) {
                case "YEAR":
                    return new YearTransform(field);
                case "MONTH":
                    return new MonthTransform(field);
                case "DAY":
                case "DAYOFMONTH":
                    return new DayTransform(field);
                case "HOUR":
                    return new HourTransform(field);
                case "MINUTE":
                    return new MinuteTransform(field);
                case "SECOND":
                    return new SecondTransform(field);
                case "QUARTER":
                    return new QuarterTransform(field);
                case "DAYOFWEEK":
                    return new DayOfWeekTransform(field);
                case "DAYOFYEAR":
                    return new DayOfYearTransform(field);
                case "WEEKDAY":
                    return new WeekdayTransform(field);
                case "WEEKOFYEAR":
                    return new WeekTransform(field);
                default:
                    throw new IllegalArgumentException("Unsupported generation function: " + name);
            }
        }
    }
}
