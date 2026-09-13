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
 * {@code [a, b]} each stay one token.
 *
 * <p>Quotes and backslashes are this level's syntax, so they are removed at depth zero and kept
 * verbatim inside a nested literal, where they are the inner level's syntax and the rule for that
 * element parses them again. Whether they appeared at depth zero is remembered: quoting or escaping
 * is how the literal text {@code null} and the empty string are written, which are otherwise
 * unrepresentable.
 *
 * <p>Whitespace around a token is dropped; whitespace inside quotes is kept.
 */
class TokenSplitter {

    /** One token of a literal body, plus whether its value was written as a literal. */
    static class Token {

        private final String value;
        private final boolean literal;

        Token(String value, boolean literal) {
            this.value = value;
            this.literal = literal;
        }

        String value() {
            return value;
        }

        /** Whether quotes or an escape made this a literal string rather than a bare word. */
        boolean literal() {
            return literal;
        }
    }

    private TokenSplitter() {}

    static List<Token> split(String content) {
        List<Token> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Stack<Character> bracketStack = new Stack<>();
        boolean inQuotes = false;
        boolean escaped = false;
        boolean literal = false;
        // length of current up to the last character that was not unquoted whitespace
        int end = 0;

        for (char c : content.toCharArray()) {
            boolean nested = !bracketStack.isEmpty();
            if (escaped) {
                // the escapee stands for itself and is never read as syntax
                escaped = false;
                current.append(c);
                end = current.length();
                continue;
            }
            if (c == '\\') {
                escaped = true;
                if (nested) {
                    // the inner rule has to see the escape to protect its own separators
                    current.append(c);
                    end = current.length();
                } else {
                    literal = true;
                }
                continue;
            }
            if (c == '"') {
                inQuotes = !inQuotes;
                if (nested) {
                    current.append(c);
                    end = current.length();
                } else {
                    literal = true;
                }
                continue;
            }
            if (!inQuotes) {
                if (StringUtils.isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (StringUtils.isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addToken(tokens, current, end, literal);
                    current.setLength(0);
                    end = 0;
                    literal = false;
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

        addToken(tokens, current, end, literal);
        return tokens;
    }

    private static void addToken(
            List<Token> tokens, StringBuilder current, int end, boolean literal) {
        // whitespace is not part of a token, so one made only of whitespace was never written;
        // quoting is how an empty value is written
        if (end > 0 || literal) {
            tokens.add(new Token(current.substring(0, end), literal));
        }
    }
}
