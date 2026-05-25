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

package org.apache.paimon.cli;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Recursive descent parser for SQL-like WHERE expressions. Supports: =, !=, <>, <, >, <=, >=, IS
 * NULL, IS NOT NULL, IN (...), BETWEEN ... AND ..., LIKE, AND, OR, parentheses.
 */
public class PredicateParser {

    private final RowType rowType;
    private final PredicateBuilder builder;
    private final List<String> fieldNames;

    private String input;
    private int pos;

    public PredicateParser(RowType rowType) {
        this.rowType = rowType;
        this.builder = new PredicateBuilder(rowType);
        this.fieldNames = rowType.getFieldNames();
    }

    public Predicate parse(String expression) {
        this.input = expression;
        this.pos = 0;
        Predicate result = parseOr();
        skipWhitespace();
        if (pos < input.length()) {
            throw new IllegalArgumentException(
                    "Unexpected token at position " + pos + ": " + input.substring(pos));
        }
        return result;
    }

    private Predicate parseOr() {
        Predicate left = parseAnd();
        while (true) {
            skipWhitespace();
            if (matchKeyword("OR")) {
                Predicate right = parseAnd();
                left = PredicateBuilder.or(left, right);
            } else {
                break;
            }
        }
        return left;
    }

    private Predicate parseAnd() {
        Predicate left = parseAtom();
        while (true) {
            skipWhitespace();
            if (matchKeyword("AND")) {
                Predicate right = parseAtom();
                left = PredicateBuilder.and(left, right);
            } else {
                break;
            }
        }
        return left;
    }

    private Predicate parseAtom() {
        skipWhitespace();
        if (pos < input.length() && input.charAt(pos) == '(') {
            pos++;
            Predicate inner = parseOr();
            skipWhitespace();
            expect(')');
            return inner;
        }

        String field = parseIdentifier();
        int idx = fieldIndex(field);
        DataType fieldType = rowType.getTypeAt(idx);

        skipWhitespace();

        if (matchKeyword("IS")) {
            skipWhitespace();
            if (matchKeyword("NOT")) {
                skipWhitespace();
                expectKeyword("NULL");
                return builder.isNotNull(idx);
            } else {
                expectKeyword("NULL");
                return builder.isNull(idx);
            }
        }

        if (matchKeyword("NOT")) {
            skipWhitespace();
            if (matchKeyword("IN")) {
                List<Object> values = parseInList(fieldType);
                return builder.notIn(idx, values);
            }
            throw new IllegalArgumentException("Expected IN after NOT at position " + pos);
        }

        if (matchKeyword("IN")) {
            List<Object> values = parseInList(fieldType);
            return builder.in(idx, values);
        }

        if (matchKeyword("BETWEEN")) {
            skipWhitespace();
            Object lower = castLiteral(parseLiteral(), fieldType);
            skipWhitespace();
            expectKeyword("AND");
            skipWhitespace();
            Object upper = castLiteral(parseLiteral(), fieldType);
            return builder.between(idx, lower, upper);
        }

        if (matchKeyword("LIKE")) {
            skipWhitespace();
            Object pattern = castLiteral(parseLiteral(), fieldType);
            return builder.like(idx, pattern);
        }

        String op = parseOperator();
        skipWhitespace();
        Object value = castLiteral(parseLiteral(), fieldType);

        switch (op) {
            case "=":
                return builder.equal(idx, value);
            case "!=":
            case "<>":
                return builder.notEqual(idx, value);
            case "<":
                return builder.lessThan(idx, value);
            case "<=":
                return builder.lessOrEqual(idx, value);
            case ">":
                return builder.greaterThan(idx, value);
            case ">=":
                return builder.greaterOrEqual(idx, value);
            default:
                throw new IllegalArgumentException("Unknown operator: " + op);
        }
    }

    private List<Object> parseInList(DataType fieldType) {
        skipWhitespace();
        expect('(');
        List<Object> values = new ArrayList<>();
        while (true) {
            skipWhitespace();
            values.add(castLiteral(parseLiteral(), fieldType));
            skipWhitespace();
            if (pos < input.length() && input.charAt(pos) == ',') {
                pos++;
            } else {
                break;
            }
        }
        expect(')');
        return values;
    }

    private String parseIdentifier() {
        skipWhitespace();
        int start = pos;
        while (pos < input.length()
                && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
            pos++;
        }
        if (pos == start) {
            throw new IllegalArgumentException("Expected identifier at position " + pos);
        }
        return input.substring(start, pos);
    }

    private String parseOperator() {
        skipWhitespace();
        if (pos + 1 < input.length()) {
            String two = input.substring(pos, pos + 2);
            if ("!=".equals(two) || "<>".equals(two) || "<=".equals(two) || ">=".equals(two)) {
                pos += 2;
                return two;
            }
        }
        if (pos < input.length()) {
            char c = input.charAt(pos);
            if (c == '=' || c == '<' || c == '>') {
                pos++;
                return String.valueOf(c);
            }
        }
        throw new IllegalArgumentException("Expected operator at position " + pos);
    }

    private String parseLiteral() {
        skipWhitespace();
        if (pos >= input.length()) {
            throw new IllegalArgumentException("Expected literal at end of input");
        }
        char c = input.charAt(pos);
        if (c == '\'') {
            pos++;
            StringBuilder sb = new StringBuilder();
            while (pos < input.length()) {
                char ch = input.charAt(pos);
                if (ch == '\'') {
                    if (pos + 1 < input.length() && input.charAt(pos + 1) == '\'') {
                        sb.append('\'');
                        pos += 2;
                    } else {
                        pos++;
                        break;
                    }
                } else {
                    sb.append(ch);
                    pos++;
                }
            }
            return "'" + sb.toString() + "'";
        }
        int start = pos;
        if (c == '-' || c == '+') {
            pos++;
        }
        while (pos < input.length()
                && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.')) {
            pos++;
        }
        if (pos == start) {
            throw new IllegalArgumentException("Expected literal at position " + pos);
        }
        return input.substring(start, pos);
    }

    private Object castLiteral(String literal, DataType type) {
        if (literal.startsWith("'") && literal.endsWith("'")) {
            String strValue = literal.substring(1, literal.length() - 1);
            DataTypeRoot root = type.getTypeRoot();
            switch (root) {
                case VARCHAR:
                case CHAR:
                    return BinaryString.fromString(strValue);
                case DATE:
                    return LocalDate.parse(strValue).toEpochDay();
                default:
                    return BinaryString.fromString(strValue);
            }
        }
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case TINYINT:
                return Byte.parseByte(literal);
            case SMALLINT:
                return Short.parseShort(literal);
            case INTEGER:
                return Integer.parseInt(literal);
            case BIGINT:
                return Long.parseLong(literal);
            case FLOAT:
                return Float.parseFloat(literal);
            case DOUBLE:
                return Double.parseDouble(literal);
            case DECIMAL:
                return BigDecimal.valueOf(Double.parseDouble(literal));
            case BOOLEAN:
                return Boolean.parseBoolean(literal);
            default:
                return BinaryString.fromString(literal);
        }
    }

    private int fieldIndex(String name) {
        int idx = fieldNames.indexOf(name);
        if (idx < 0) {
            throw new IllegalArgumentException("Unknown field: " + name);
        }
        return idx;
    }

    private void skipWhitespace() {
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }
    }

    private boolean matchKeyword(String keyword) {
        int savedPos = pos;
        int len = keyword.length();
        if (pos + len > input.length()) {
            return false;
        }
        if (input.substring(pos, pos + len).equalsIgnoreCase(keyword)) {
            if (pos + len >= input.length()
                    || !Character.isLetterOrDigit(input.charAt(pos + len))) {
                pos += len;
                return true;
            }
        }
        pos = savedPos;
        return false;
    }

    private void expectKeyword(String keyword) {
        if (!matchKeyword(keyword)) {
            throw new IllegalArgumentException("Expected '" + keyword + "' at position " + pos);
        }
    }

    private void expect(char c) {
        skipWhitespace();
        if (pos >= input.length() || input.charAt(pos) != c) {
            throw new IllegalArgumentException("Expected '" + c + "' at position " + pos);
        }
        pos++;
    }
}
