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

package org.apache.paimon.types;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parser for creating instances of {@link org.apache.paimon.types.DataType} from a serialized
 * string created with {@link org.apache.paimon.types.DataType#serializeJson}.
 */
public final class DataTypeJsonParser {

    public static DataField parseDataField(JsonNode json) {
        int id = json.get("id").asInt();
        String name = json.get("name").asText();
        DataType type = parseDataType(json.get("type"));
        JsonNode descriptionNode = json.get("description");
        String description = null;
        if (descriptionNode != null) {
            description = descriptionNode.asText();
        }
        return new DataField(id, name, type, description);
    }

    public static DataType parseDataType(JsonNode json) {
        if (json.isTextual()) {
            return parseAtomicTypeSQLString(json.asText());
        } else if (json.isObject()) {
            String typeString = json.get("type").asText();
            if (typeString.startsWith("ARRAY")) {
                DataType element = parseDataType(json.get("element"));
                return new ArrayType(!typeString.contains("NOT NULL"), element);
            } else if (typeString.startsWith("MULTISET")) {
                DataType element = parseDataType(json.get("element"));
                return new MultisetType(!typeString.contains("NOT NULL"), element);
            } else if (typeString.startsWith("MAP")) {
                DataType key = parseDataType(json.get("key"));
                DataType value = parseDataType(json.get("value"));
                return new MapType(!typeString.contains("NOT NULL"), key, value);
            } else if (typeString.startsWith("ROW")) {
                JsonNode fieldArray = json.get("fields");
                Iterator<JsonNode> iterator = fieldArray.elements();
                List<DataField> fields = new ArrayList<>(fieldArray.size());
                while (iterator.hasNext()) {
                    fields.add(parseDataField(iterator.next()));
                }
                return new RowType(!typeString.contains("NOT NULL"), fields);
            }
        }

        throw new IllegalArgumentException("Can not parse: " + json);
    }

    public static DataType parseAtomicTypeSQLString(String string) {
        List<Token> tokens = tokenize(string);
        TokenParser converter = new TokenParser(string, tokens);
        return converter.parseTokens();
    }

    // --------------------------------------------------------------------------------------------
    // Tokenizer
    // --------------------------------------------------------------------------------------------

    private static final char CHAR_BEGIN_SUBTYPE = '<';
    private static final char CHAR_END_SUBTYPE = '>';
    private static final char CHAR_BEGIN_PARAMETER = '(';
    private static final char CHAR_END_PARAMETER = ')';
    private static final char CHAR_LIST_SEPARATOR = ',';
    private static final char CHAR_STRING = '\'';
    private static final char CHAR_IDENTIFIER = '`';
    private static final char CHAR_DOT = '.';

    private static boolean isDelimiter(char character) {
        return Character.isWhitespace(character)
                || character == CHAR_BEGIN_SUBTYPE
                || character == CHAR_END_SUBTYPE
                || character == CHAR_BEGIN_PARAMETER
                || character == CHAR_END_PARAMETER
                || character == CHAR_LIST_SEPARATOR
                || character == CHAR_DOT;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static List<Token> tokenize(String chars) {
        final List<Token> tokens = new ArrayList<>();
        final StringBuilder builder = new StringBuilder();
        for (int cursor = 0; cursor < chars.length(); cursor++) {
            char curChar = chars.charAt(cursor);
            switch (curChar) {
                case CHAR_BEGIN_SUBTYPE:
                    tokens.add(
                            new Token(
                                    TokenType.BEGIN_SUBTYPE,
                                    cursor,
                                    Character.toString(CHAR_BEGIN_SUBTYPE)));
                    break;
                case CHAR_END_SUBTYPE:
                    tokens.add(
                            new Token(
                                    TokenType.END_SUBTYPE,
                                    cursor,
                                    Character.toString(CHAR_END_SUBTYPE)));
                    break;
                case CHAR_BEGIN_PARAMETER:
                    tokens.add(
                            new Token(
                                    TokenType.BEGIN_PARAMETER,
                                    cursor,
                                    Character.toString(CHAR_BEGIN_PARAMETER)));
                    break;
                case CHAR_END_PARAMETER:
                    tokens.add(
                            new Token(
                                    TokenType.END_PARAMETER,
                                    cursor,
                                    Character.toString(CHAR_END_PARAMETER)));
                    break;
                case CHAR_LIST_SEPARATOR:
                    tokens.add(
                            new Token(
                                    TokenType.LIST_SEPARATOR,
                                    cursor,
                                    Character.toString(CHAR_LIST_SEPARATOR)));
                    break;
                case CHAR_DOT:
                    tokens.add(
                            new Token(
                                    TokenType.IDENTIFIER_SEPARATOR,
                                    cursor,
                                    Character.toString(CHAR_DOT)));
                    break;
                case CHAR_STRING:
                    builder.setLength(0);
                    cursor = consumeEscaped(builder, chars, cursor, CHAR_STRING);
                    tokens.add(new Token(TokenType.LITERAL_STRING, cursor, builder.toString()));
                    break;
                case CHAR_IDENTIFIER:
                    builder.setLength(0);
                    cursor = consumeEscaped(builder, chars, cursor, CHAR_IDENTIFIER);
                    tokens.add(new Token(TokenType.IDENTIFIER, cursor, builder.toString()));
                    break;
                default:
                    if (Character.isWhitespace(curChar)) {
                        continue;
                    }
                    if (isDigit(curChar)) {
                        builder.setLength(0);
                        cursor = consumeInt(builder, chars, cursor);
                        tokens.add(new Token(TokenType.LITERAL_INT, cursor, builder.toString()));
                        break;
                    }
                    builder.setLength(0);
                    cursor = consumeIdentifier(builder, chars, cursor);
                    final String token = builder.toString();
                    final String normalizedToken = token.toUpperCase();
                    if (KEYWORDS.contains(normalizedToken)) {
                        tokens.add(new Token(TokenType.KEYWORD, cursor, normalizedToken));
                    } else {
                        tokens.add(new Token(TokenType.IDENTIFIER, cursor, token));
                    }
            }
        }

        return tokens;
    }

    private static int consumeEscaped(
            StringBuilder builder, String chars, int cursor, char delimiter) {
        // skip delimiter
        cursor++;
        for (; chars.length() > cursor; cursor++) {
            final char curChar = chars.charAt(cursor);
            if (curChar == delimiter
                    && cursor + 1 < chars.length()
                    && chars.charAt(cursor + 1) == delimiter) {
                // escaping of the escaping char e.g. "'Hello '' World'"
                cursor++;
                builder.append(curChar);
            } else if (curChar == delimiter) {
                break;
            } else {
                builder.append(curChar);
            }
        }
        return cursor;
    }

    private static int consumeInt(StringBuilder builder, String chars, int cursor) {
        for (; chars.length() > cursor && isDigit(chars.charAt(cursor)); cursor++) {
            builder.append(chars.charAt(cursor));
        }
        return cursor - 1;
    }

    private static int consumeIdentifier(StringBuilder builder, String chars, int cursor) {
        for (; cursor < chars.length() && !isDelimiter(chars.charAt(cursor)); cursor++) {
            builder.append(chars.charAt(cursor));
        }
        return cursor - 1;
    }

    private enum TokenType {
        // e.g. "ROW<"
        BEGIN_SUBTYPE,

        // e.g. "ROW<..>"
        END_SUBTYPE,

        // e.g. "CHAR("
        BEGIN_PARAMETER,

        // e.g. "CHAR(...)"
        END_PARAMETER,

        // e.g. "ROW<INT,"
        LIST_SEPARATOR,

        // e.g. "ROW<name INT 'Comment'"
        LITERAL_STRING,

        // CHAR(12
        LITERAL_INT,

        // e.g. "CHAR" or "TO"
        KEYWORD,

        // e.g. "ROW<name" or "myCatalog.myDatabase"
        IDENTIFIER,

        // e.g. "myCatalog.myDatabase."
        IDENTIFIER_SEPARATOR
    }

    private enum Keyword {
        CHAR,
        VARCHAR,
        STRING,
        BOOLEAN,
        BINARY,
        VARBINARY,
        BYTES,
        DECIMAL,
        NUMERIC,
        DEC,
        TINYINT,
        SMALLINT,
        INT,
        INTEGER,
        BIGINT,
        FLOAT,
        DOUBLE,
        PRECISION,
        DATE,
        TIME,
        WITH,
        WITHOUT,
        LOCAL,
        ZONE,
        TIMESTAMP,
        TIMESTAMP_LTZ,
        INTERVAL,
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        TO,
        ARRAY,
        MULTISET,
        MAP,
        ROW,
        NULL,
        RAW,
        LEGACY,
        VARIANT,
        NOT
    }

    private static final Set<String> KEYWORDS =
            Stream.of(Keyword.values())
                    .map(k -> k.toString().toUpperCase())
                    .collect(Collectors.toSet());

    private static class Token {
        public final TokenType type;
        public final int cursorPosition;
        public final String value;

        public Token(TokenType type, int cursorPosition, String value) {
            this.type = type;
            this.cursorPosition = cursorPosition;
            this.value = value;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Token Parsing
    // --------------------------------------------------------------------------------------------

    private static class TokenParser {

        private final String inputString;

        private final List<Token> tokens;

        private int lastValidToken;

        private int currentToken;

        public TokenParser(String inputString, List<Token> tokens) {
            this.inputString = inputString;
            this.tokens = tokens;
            this.lastValidToken = -1;
            this.currentToken = -1;
        }

        private DataType parseTokens() {
            final DataType type = parseTypeWithNullability();
            if (hasRemainingTokens()) {
                nextToken();
                throw parsingError("Unexpected token: " + token().value);
            }
            return type;
        }

        private int lastCursor() {
            if (lastValidToken < 0) {
                return 0;
            }
            return tokens.get(lastValidToken).cursorPosition + 1;
        }

        private IllegalArgumentException parsingError(String cause, @Nullable Throwable t) {
            return new IllegalArgumentException(
                    String.format(
                            "Could not parse type at position %d: %s\n Input type string: %s",
                            lastCursor(), cause, inputString),
                    t);
        }

        private IllegalArgumentException parsingError(String cause) {
            return parsingError(cause, null);
        }

        private boolean hasRemainingTokens() {
            return currentToken + 1 < tokens.size();
        }

        private Token token() {
            return tokens.get(currentToken);
        }

        private int tokenAsInt() {
            try {
                return Integer.parseInt(token().value);
            } catch (NumberFormatException e) {
                throw parsingError("Invalid integer value.", e);
            }
        }

        private Keyword tokenAsKeyword() {
            return Keyword.valueOf(token().value);
        }

        private void nextToken() {
            this.currentToken++;
            if (currentToken >= tokens.size()) {
                throw parsingError("Unexpected end.");
            }
            lastValidToken = this.currentToken - 1;
        }

        private void nextToken(TokenType type) {
            nextToken();
            final Token token = token();
            if (token.type != type) {
                throw parsingError("<" + type.name() + "> expected but was <" + token.type + ">.");
            }
        }

        private void nextToken(Keyword keyword) {
            nextToken(TokenType.KEYWORD);
            final Token token = token();
            if (!keyword.equals(Keyword.valueOf(token.value))) {
                throw parsingError(
                        "Keyword '" + keyword + "' expected but was '" + token.value + "'.");
            }
        }

        private boolean hasNextToken(TokenType... types) {
            if (currentToken + types.length + 1 > tokens.size()) {
                return false;
            }
            for (int i = 0; i < types.length; i++) {
                final Token lookAhead = tokens.get(currentToken + i + 1);
                if (lookAhead.type != types[i]) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasNextToken(Keyword... keywords) {
            if (currentToken + keywords.length + 1 > tokens.size()) {
                return false;
            }
            for (int i = 0; i < keywords.length; i++) {
                final Token lookAhead = tokens.get(currentToken + i + 1);
                if (lookAhead.type != TokenType.KEYWORD
                        || keywords[i] != Keyword.valueOf(lookAhead.value)) {
                    return false;
                }
            }
            return true;
        }

        private boolean parseNullability() {
            // "NOT NULL"
            if (hasNextToken(Keyword.NOT, Keyword.NULL)) {
                nextToken(Keyword.NOT);
                nextToken(Keyword.NULL);
                return false;
            }
            // explicit "NULL"
            else if (hasNextToken(Keyword.NULL)) {
                nextToken(Keyword.NULL);
                return true;
            }
            // implicit "NULL"
            return true;
        }

        private DataType parseTypeWithNullability() {
            DataType dataType = parseTypeByKeyword().copy(parseNullability());

            // special case: suffix notation for ARRAY and MULTISET types
            if (hasNextToken(Keyword.ARRAY)) {
                nextToken(Keyword.ARRAY);
                return new ArrayType(dataType).copy(parseNullability());
            } else if (hasNextToken(Keyword.MULTISET)) {
                nextToken(Keyword.MULTISET);
                return new MultisetType(dataType).copy(parseNullability());
            }

            return dataType;
        }

        private DataType parseTypeByKeyword() {
            nextToken(TokenType.KEYWORD);
            switch (tokenAsKeyword()) {
                case CHAR:
                    return parseCharType();
                case VARCHAR:
                    return parseVarCharType();
                case STRING:
                    return VarCharType.STRING_TYPE;
                case BOOLEAN:
                    return new BooleanType();
                case BINARY:
                    return parseBinaryType();
                case VARBINARY:
                    return parseVarBinaryType();
                case BYTES:
                    return new VarBinaryType(VarBinaryType.MAX_LENGTH);
                case DECIMAL:
                case NUMERIC:
                case DEC:
                    return parseDecimalType();
                case TINYINT:
                    return new TinyIntType();
                case SMALLINT:
                    return new SmallIntType();
                case INT:
                case INTEGER:
                    return new IntType();
                case BIGINT:
                    return new BigIntType();
                case FLOAT:
                    return new FloatType();
                case DOUBLE:
                    return parseDoubleType();
                case DATE:
                    return new DateType();
                case TIME:
                    return parseTimeType();
                case TIMESTAMP:
                    return parseTimestampType();
                case TIMESTAMP_LTZ:
                    return parseTimestampLtzType();
                case VARIANT:
                    return new VariantType();
                default:
                    throw parsingError("Unsupported type: " + token().value);
            }
        }

        private int parseStringType() {
            // explicit length
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                final int length = tokenAsInt();
                nextToken(TokenType.END_PARAMETER);
                return length;
            }
            // implicit length
            return -1;
        }

        private DataType parseCharType() {
            final int length = parseStringType();
            if (length < 0) {
                return new CharType();
            } else {
                return new CharType(length);
            }
        }

        private DataType parseVarCharType() {
            final int length = parseStringType();
            if (length < 0) {
                return new VarCharType();
            } else {
                return new VarCharType(length);
            }
        }

        private DataType parseBinaryType() {
            final int length = parseStringType();
            if (length < 0) {
                return new BinaryType();
            } else {
                return new BinaryType(length);
            }
        }

        private DataType parseVarBinaryType() {
            final int length = parseStringType();
            if (length < 0) {
                return new VarBinaryType();
            } else {
                return new VarBinaryType(length);
            }
        }

        private DataType parseDecimalType() {
            int precision = DecimalType.DEFAULT_PRECISION;
            int scale = DecimalType.DEFAULT_SCALE;
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                precision = tokenAsInt();
                if (hasNextToken(TokenType.LIST_SEPARATOR)) {
                    nextToken(TokenType.LIST_SEPARATOR);
                    nextToken(TokenType.LITERAL_INT);
                    scale = tokenAsInt();
                }
                nextToken(TokenType.END_PARAMETER);
            }
            return new DecimalType(precision, scale);
        }

        private DataType parseDoubleType() {
            if (hasNextToken(Keyword.PRECISION)) {
                nextToken(Keyword.PRECISION);
            }
            return new DoubleType();
        }

        private DataType parseTimeType() {
            int precision = parseOptionalPrecision(TimeType.DEFAULT_PRECISION);
            if (hasNextToken(Keyword.WITHOUT)) {
                nextToken(Keyword.WITHOUT);
                nextToken(Keyword.TIME);
                nextToken(Keyword.ZONE);
            }
            return new TimeType(precision);
        }

        private DataType parseTimestampType() {
            int precision = parseOptionalPrecision(TimestampType.DEFAULT_PRECISION);
            if (hasNextToken(Keyword.WITHOUT)) {
                nextToken(Keyword.WITHOUT);
                nextToken(Keyword.TIME);
                nextToken(Keyword.ZONE);
            } else if (hasNextToken(Keyword.WITH)) {
                nextToken(Keyword.WITH);
                if (hasNextToken(Keyword.LOCAL)) {
                    nextToken(Keyword.LOCAL);
                    nextToken(Keyword.TIME);
                    nextToken(Keyword.ZONE);
                    return new LocalZonedTimestampType(precision);
                }
            }
            return new TimestampType(precision);
        }

        private DataType parseTimestampLtzType() {
            int precision = parseOptionalPrecision(LocalZonedTimestampType.DEFAULT_PRECISION);
            return new LocalZonedTimestampType(precision);
        }

        private int parseOptionalPrecision(int defaultPrecision) {
            int precision = defaultPrecision;
            if (hasNextToken(TokenType.BEGIN_PARAMETER)) {
                nextToken(TokenType.BEGIN_PARAMETER);
                nextToken(TokenType.LITERAL_INT);
                precision = tokenAsInt();
                nextToken(TokenType.END_PARAMETER);
            }
            return precision;
        }
    }
}
