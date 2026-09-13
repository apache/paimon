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

package org.apache.paimon.casting;

import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Splits the comma-separated body of an array or row literal into its tokens, honouring quotes,
 * escapes and nesting.
 *
 * <p>A separator only separates outside quotes and at bracket depth zero, so {@code "a,b"} and
 * {@code [a, b]} each stay one token. Quotes and backslashes are grouping syntax and do not survive
 * into the value, but whether a token was quoted does: quoting is how the literal text {@code null}
 * and the empty string are written, which are otherwise unrepresentable.
 *
 * <p>Whitespace around a token is dropped; whitespace inside quotes is kept.
 */
class TokenSplitter {

    /** One token of a literal body, plus whether quotes contributed to it. */
    static class Token {

        private final String value;
        private final boolean quoted;

        Token(String value, boolean quoted) {
            this.value = value;
            this.quoted = quoted;
        }

        String value() {
            return value;
        }

        /** Whether the token was written with quotes, which makes its value a literal string. */
        boolean quoted() {
            return quoted;
        }
    }

    private TokenSplitter() {}

    static List<Token> split(String content) {
        List<Token> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Stack<Character> bracketStack = new Stack<>();
        boolean inQuotes = false;
        boolean escaped = false;
        boolean quoted = false;
        // length of current up to the last character that was not unquoted whitespace
        int end = 0;

        for (char c : content.toCharArray()) {
            if (escaped) {
                // an escaped character stands for itself; the backslash is syntax
                escaped = false;
                current.append(c);
                end = current.length();
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inQuotes = !inQuotes;
                quoted = true;
                continue;
            }
            if (!inQuotes) {
                if (StringUtils.isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (StringUtils.isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addToken(tokens, current, end, quoted);
                    current.setLength(0);
                    end = 0;
                    quoted = false;
                    continue;
                } else if (Character.isWhitespace(c) && end == 0) {
                    // leading whitespace outside quotes is not part of the token
                    continue;
                }
            }
            current.append(c);
            if (inQuotes || !Character.isWhitespace(c)) {
                end = current.length();
            }
        }

        addToken(tokens, current, end, quoted);
        return tokens;
    }

    private static void addToken(
            List<Token> tokens, StringBuilder current, int end, boolean quoted) {
        // an empty unquoted token is absent rather than empty, so a trailing or doubled
        // separator does not invent a value
        if (end > 0 || quoted) {
            tokens.add(new Token(current.substring(0, end), quoted));
        }
    }
}
