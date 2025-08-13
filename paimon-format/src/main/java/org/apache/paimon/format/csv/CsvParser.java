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

package org.apache.paimon.format.csv;

import java.util.ArrayList;
import java.util.List;

/**
 * CSV parser for parsing CSV lines into fields. This parser handles:
 *
 * <ul>
 *   <li>Standard CSV fields with quote escaping
 *   <li>JSON arrays and objects (preserves structure)
 *   <li>Mixed CSV and JSON content in the same line
 * </ul>
 */
public class CsvParser {

    /**
     * Split one CSV line into fields with optimized performance.
     *
     * <p>Field handling rules:
     *
     * <ul>
     *   <li>Normal fields: remove outer quotes and unescape double quotes ("")
     *   <li>JSON fields: preserve structure and quotes as needed
     * </ul>
     *
     * @param line the CSV line to parse
     * @param delimiter field delimiter character
     * @param quote quote character for escaping
     * @return list of parsed field values
     */
    public static List<String> splitCsvLine(
            int fieldSize, String line, char delimiter, char quote) {
        if (line == null || line.isEmpty()) {
            return new ArrayList<>(0);
        }

        final char[] chars = line.toCharArray();
        final int length = chars.length;

        // Estimate field count for better ArrayList sizing
        List<String> fields = new ArrayList<>(fieldSize);

        // Calculate optimal buffer capacity
        StringBuilder buffer = new StringBuilder(length / fieldSize + 16);

        // Parser state variables
        boolean inCsvQuotes = false;
        boolean fieldHadOuterQuotes = false;
        boolean inJsonString = false;
        boolean escaped = false;
        int braceDepth = 0; // Track {} nesting
        int bracketDepth = 0; // Track [] nesting

        for (int i = 0; i < length; i++) {
            char c = chars[i];

            // Handle escape sequences
            if (escaped) {
                buffer.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                buffer.append(c);
                continue;
            }

            // Handle JSON string quotes (inside JSON structures)
            if (!inCsvQuotes && (braceDepth > 0 || bracketDepth > 0) && c == quote) {
                inJsonString = !inJsonString;
                buffer.append(c);
                continue;
            }

            // Handle CSV field quotes
            if (c == quote && !inJsonString) {
                if (inCsvQuotes) {
                    // Check for escaped quote ("")
                    if (i + 1 < length && chars[i + 1] == quote) {
                        buffer.append(quote).append(quote);
                        i++; // Skip next quote
                    } else {
                        inCsvQuotes = false;
                        fieldHadOuterQuotes = true;
                        buffer.append(quote);
                    }
                } else {
                    inCsvQuotes = true;
                    buffer.append(quote);
                }
                continue;
            }

            // Handle field delimiter
            if (c == delimiter
                    && !inCsvQuotes
                    && !inJsonString
                    && braceDepth == 0
                    && bracketDepth == 0) {

                addFieldToList(fields, buffer, fieldHadOuterQuotes, quote);
                buffer.setLength(0); // Reset buffer efficiently
                fieldHadOuterQuotes = false;
                continue;
            }

            // Add character to buffer
            buffer.append(c);

            // Track JSON structure depth (only outside CSV quotes and JSON strings)
            if (!inCsvQuotes && !inJsonString) {
                switch (c) {
                    case '{':
                        braceDepth++;
                        break;
                    case '}':
                        braceDepth = Math.max(0, braceDepth - 1);
                        break;
                    case '[':
                        bracketDepth++;
                        break;
                    case ']':
                        bracketDepth = Math.max(0, bracketDepth - 1);
                        break;
                }
            }
        }

        // Add the final field
        addFieldToList(fields, buffer, fieldHadOuterQuotes, quote);
        return fields;
    }

    /**
     * Add the current field to the result list based on field type and quoting rules.
     *
     * @param fields the list to add the field to
     * @param buffer the current field content
     * @param hadOuterQuotes whether the field had outer quotes
     * @param quote the quote character used
     */
    private static void addFieldToList(
            List<String> fields, StringBuilder buffer, boolean hadOuterQuotes, char quote) {
        if (buffer.length() == 0) {
            fields.add("");
            return;
        }

        String field = buffer.toString();

        if (!hadOuterQuotes) {
            // Unquoted field - add as-is (may be JSON)
            fields.add(field);
            return;
        }

        // Quoted field - determine if JSON or regular
        String innerContent = stripOuterQuotes(field, quote);
        if (isJson(innerContent)) {
            // JSON field - preserve original quoting
            fields.add(field);
        } else {
            // Regular field - remove quotes and unescape
            fields.add(unescapeDoubleQuotes(innerContent, quote));
        }
    }

    /**
     * Remove outer quotes if present, otherwise return original string.
     *
     * @param s the string to process
     * @param quote the quote character
     * @return string with outer quotes removed if present
     */
    private static String stripOuterQuotes(String s, char quote) {
        int len = s.length();
        if (len >= 2 && s.charAt(0) == quote && s.charAt(len - 1) == quote) {
            return s.substring(1, len - 1);
        }
        return s;
    }

    /**
     * Replace CSV escaped quotes ("") with single quotes.
     *
     * @param s the string to unescape
     * @param quote the quote character
     * @return string with double quotes unescaped
     */
    private static String unescapeDoubleQuotes(String s, char quote) {
        // Fast path - no quotes to unescape
        if (s.indexOf(quote) == -1) {
            return s;
        }

        StringBuilder result = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == quote && i + 1 < s.length() && s.charAt(i + 1) == quote) {
                result.append(quote);
                i++; // Skip the second quote
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /**
     * Check if a string represents JSON data (array or object). Ignores leading/trailing
     * whitespace.
     *
     * @param s the string to check
     * @return true if the string looks like JSON (starts with { or [ and ends with } or ])
     */
    private static boolean isJson(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }

        // Find first and last non-whitespace characters
        int start = 0, end = s.length() - 1;
        while (start <= end && isWhitespace(s.charAt(start))) {
            start++;
        }
        if (start > end) {
            return false;
        }
        while (end >= start && isWhitespace(s.charAt(end))) {
            end--;
        }

        char first = s.charAt(start);
        char last = s.charAt(end);
        return (first == '{' && last == '}') || (first == '[' && last == ']');
    }

    /**
     * Check if character is whitespace (space, tab, newline, carriage return).
     *
     * @param c the character to check
     * @return true if the character is whitespace
     */
    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\r' || c == '\n';
    }
}
