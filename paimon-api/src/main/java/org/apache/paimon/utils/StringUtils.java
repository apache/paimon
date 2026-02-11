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

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Utils for strings. */
public class StringUtils {

    private static final char[] HEX_CHARS = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    /**
     * Represents a failed index search.
     *
     * @since 2.1
     */
    public static final int INDEX_NOT_FOUND = -1;

    /** The empty String {@code ""}. */
    public static final String EMPTY = "";

    /** Default maximum number of fields for truncated string representation. */
    public static final int DEFAULT_MAX_FIELDS = 25;

    /**
     * Checks if the string is null, empty, or contains only whitespace characters. A whitespace
     * character is defined via {@link Character#isWhitespace(char)}.
     *
     * @param str The string to check
     * @return True, if the string is null or blank, false otherwise.
     */
    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @param start start index, inclusively
     * @param end end index, exclusively
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }

        return new String(out);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string representation of the
     * bytes.
     *
     * @param bytes the bytes to convert in a hex string
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    /**
     * Given an array of bytes it will convert the bytes to a binary (0-1) string representation of
     * the bytes.
     *
     * @param bytes the bytes to be converted
     * @return binary string representation of the byte array
     */
    public static String bytesToBinaryString(final byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        return result.toString();
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only
     * characters that can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @return A random String.
     */
    public static String getRandomString(Random rnd, int minLength, int maxLength) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(0x7fff) + 1);
        }
        return new String(data);
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only
     * characters that can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @param minValue The minimum character value to occur.
     * @param maxValue The maximum character value to occur.
     * @return A random String.
     */
    public static String getRandomString(
            Random rnd, int minLength, int maxLength, char minValue, char maxValue) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        int diff = maxValue - minValue + 1;

        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(diff) + minValue);
        }
        return new String(data);
    }

    /**
     * Returns a string consisting of a specific number of concatenated copies of an input string.
     * For example, {@code repeat("hey", 3)} returns the string {@code "heyheyhey"}.
     *
     * @param string any non-null string
     * @param count the number of times to repeat it; a nonnegative integer
     * @return a string containing {@code string} repeated {@code count} times (the empty string if
     *     {@code count} is zero)
     * @throws IllegalArgumentException if {@code count} is negative
     */
    public static String repeat(String string, int count) {
        checkNotNull(string); // eager for GWT.

        if (count <= 1) {
            checkArgument(count >= 0, "invalid count: %s", count);
            return (count == 0) ? "" : string;
        }

        // IF YOU MODIFY THE CODE HERE, you must update StringsRepeatBenchmark
        final int len = string.length();
        final long longSize = (long) len * (long) count;
        final int size = (int) longSize;
        if (size != longSize) {
            throw new ArrayIndexOutOfBoundsException("Required array size too large: " + longSize);
        }

        final char[] array = new char[size];
        string.getChars(0, len, array, 0);
        int n;
        for (n = len; n < size - n; n <<= 1) {
            System.arraycopy(array, 0, array, n, n);
        }
        System.arraycopy(array, 0, array, n, size - n);
        return new String(array);
    }

    /**
     * Replaces all occurrences of a String within another String.
     *
     * <p>A {@code null} reference passed to this method is a no-op.
     *
     * <pre>
     * StringUtils.replace(null, *, *)        = null
     * StringUtils.replace("", *, *)          = ""
     * StringUtils.replace("any", null, *)    = "any"
     * StringUtils.replace("any", *, null)    = "any"
     * StringUtils.replace("any", "", *)      = "any"
     * StringUtils.replace("aba", "a", null)  = "aba"
     * StringUtils.replace("aba", "a", "")    = "b"
     * StringUtils.replace("aba", "a", "z")   = "zbz"
     * </pre>
     *
     * @see #replace(String text, String searchString, String replacement, int max)
     * @param text text to search and replace in, may be null
     * @param searchString the String to search for, may be null
     * @param replacement the String to replace it with, may be null
     * @return the text with any replacements processed, {@code null} if null String input
     */
    public static String replace(
            final String text, final String searchString, final String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    /**
     * Replaces a String with another String inside a larger String, for the first {@code max}
     * values of the search String.
     *
     * <p>A {@code null} reference passed to this method is a no-op.
     *
     * <pre>
     * StringUtils.replace(null, *, *, *)         = null
     * StringUtils.replace("", *, *, *)           = ""
     * StringUtils.replace("any", null, *, *)     = "any"
     * StringUtils.replace("any", *, null, *)     = "any"
     * StringUtils.replace("any", "", *, *)       = "any"
     * StringUtils.replace("any", *, *, 0)        = "any"
     * StringUtils.replace("abaa", "a", null, -1) = "abaa"
     * StringUtils.replace("abaa", "a", "", -1)   = "b"
     * StringUtils.replace("abaa", "a", "z", 0)   = "abaa"
     * StringUtils.replace("abaa", "a", "z", 1)   = "zbaa"
     * StringUtils.replace("abaa", "a", "z", 2)   = "zbza"
     * StringUtils.replace("abaa", "a", "z", -1)  = "zbzz"
     * </pre>
     *
     * @param text text to search and replace in, may be null
     * @param searchString the String to search for, may be null
     * @param replacement the String to replace it with, may be null
     * @param max maximum number of values to replace, or {@code -1} if no maximum
     * @return the text with any replacements processed, {@code null} if null String input
     */
    public static String replace(
            final String text, final String searchString, final String replacement, int max) {
        if (isEmpty(text) || isEmpty(searchString) || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == INDEX_NOT_FOUND) {
            return text;
        }
        final int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = Math.max(increase, 0);
        increase *= max < 0 ? 16 : Math.min(max, 64);
        final StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != INDEX_NOT_FOUND) {
            buf.append(text, start, end).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    /**
     * Checks if a CharSequence is empty ("") or null.
     *
     * <pre>
     * StringUtils.isEmpty(null)      = true
     * StringUtils.isEmpty("")        = true
     * StringUtils.isEmpty(" ")       = false
     * StringUtils.isEmpty("bob")     = false
     * StringUtils.isEmpty("  bob  ") = false
     * </pre>
     *
     * <p>NOTE: This method changed in Lang version 2.0. It no longer trims the CharSequence. That
     * functionality is available in isNullOrWhitespaceOnly().
     *
     * @param cs the CharSequence to check, may be null
     * @return {@code true} if the CharSequence is empty or null
     * @since 3.0 Changed signature from isEmpty(String) to isEmpty(CharSequence)
     */
    public static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    public static String randomNumericString(int len) {
        StringBuilder builder = new StringBuilder();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < len; i++) {
            builder.append(rnd.nextInt(10));
        }
        return builder.toString();
    }

    /**
     * Splits the provided text into an array, separators specified. This is an alternative to using
     * StringTokenizer.
     *
     * <p>The separator is not included in the returned String array. Adjacent separators are
     * treated as one separator. For more control over the split use the StrTokenizer class.
     *
     * <p>A {@code null} input String returns {@code null}. A {@code null} separatorChars splits on
     * whitespace.
     *
     * <pre>
     * StringUtils.split(null, *)         = null
     * StringUtils.split("", *)           = []
     * StringUtils.split("abc def", null) = ["abc", "def"]
     * StringUtils.split("abc def", " ")  = ["abc", "def"]
     * StringUtils.split("abc  def", " ") = ["abc", "def"]
     * StringUtils.split("ab:cd:ef", ":") = ["ab", "cd", "ef"]
     * </pre>
     *
     * @param str the String to parse, may be null
     * @param separatorChars the characters used as the delimiters, {@code null} splits on
     *     whitespace
     * @return an array of parsed Strings, {@code null} if null String input
     */
    public static String[] split(final String str, final String separatorChars) {
        return split(str, separatorChars, -1, false);
    }

    /**
     * Performs the logic for the {@code split} and {@code splitPreserveAllTokens} methods that
     * return a maximum array length.
     *
     * @param str the String to parse, may be {@code null}
     * @param separatorChars the separate character
     * @param max the maximum number of elements to include in the array. A zero or negative value
     *     implies no limit.
     * @param preserveAllTokens if {@code true}, adjacent separators are treated as empty token
     *     separators; if {@code false}, adjacent separators are treated as one separator.
     * @return an array of parsed Strings, {@code null} if null String input
     */
    public static String[] split(
            final String str,
            final String separatorChars,
            final int max,
            final boolean preserveAllTokens) {
        // Performance tuned for 2.0 (JDK1.4)
        // Direct code is quicker than StringTokenizer.
        // Also, StringTokenizer uses isSpace() not isWhitespace()

        if (str == null) {
            return null;
        }
        final int len = str.length();
        if (len == 0) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        final List<String> list = new ArrayList<String>();
        int sizePlus1 = 1;
        int i = 0, start = 0;
        boolean match = false;
        boolean lastMatch = false;
        if (separatorChars == null) {
            // Null separator means use whitespace
            while (i < len) {
                if (Character.isWhitespace(str.charAt(i))) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {
            // Optimise 1 character case
            final char sep = separatorChars.charAt(0);
            while (i < len) {
                if (str.charAt(i) == sep) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        } else {
            // standard case
            while (i < len) {
                if (separatorChars.indexOf(str.charAt(i)) >= 0) {
                    if (match || preserveAllTokens) {
                        lastMatch = true;
                        if (sizePlus1++ == max) {
                            i = len;
                            lastMatch = false;
                        }
                        list.add(str.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                lastMatch = false;
                match = true;
                i++;
            }
        }
        if (match || preserveAllTokens && lastMatch) {
            list.add(str.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * Joins the elements of the provided {@code Iterable} into a single String containing the
     * provided elements.
     *
     * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
     * empty String ("").
     *
     * @param iterable the {@code Iterable} providing the values to join together, may be null
     * @param separator the separator character to use, null treated as ""
     * @return the joined String, {@code null} if null iterator input
     */
    public static String join(final Iterable<?> iterable, final String separator) {
        if (iterable == null) {
            return null;
        }
        return join(iterable.iterator(), separator);
    }

    /**
     * Joins the elements of the provided {@code Iterator} into a single String containing the
     * provided elements.
     *
     * <p>No delimiter is added before or after the list. A {@code null} separator is the same as an
     * empty String ("").
     *
     * @param iterator the {@code Iterator} of values to join together, may be null
     * @param separator the separator character to use, null treated as ""
     * @return the joined String, {@code null} if null iterator input
     */
    public static String join(final Iterator<?> iterator, final String separator) {

        // handle null, zero and one elements before building a buffer
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            return EMPTY;
        }
        final Object first = iterator.next();
        if (!iterator.hasNext()) {
            return Objects.toString(first);
        }

        // two or more elements
        final StringBuilder buf = new StringBuilder(256); // Java default is 16, probably too small
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            final Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        return buf.toString();
    }

    public static String quote(String str) {
        return "`" + str + "`";
    }

    public static String toLowerCaseIfNeed(String str, boolean caseSensitive) {
        return caseSensitive ? str : str.toLowerCase();
    }

    public static boolean isNumeric(final CharSequence cs) {
        if (isEmpty(cs)) {
            return false;
        }
        final int sz = cs.length();
        int startIndex = 0;

        // Handle negative sign
        if (sz > 0 && cs.charAt(0) == '-') {
            if (sz == 1) {
                return false; // Just a minus sign is not numeric
            }
            startIndex = 1;
        }

        for (int i = startIndex; i < sz; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    // A null-safe trim method.
    public static String trim(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    public static String trim(String value, String charsToTrim) {
        if (value == null || charsToTrim == null) {
            return null;
        }
        return rtrim(ltrim(value, charsToTrim), charsToTrim);
    }

    public static String ltrim(String value, String charsToTrim) {
        if (value == null || charsToTrim == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(value);
        while (sb.length() > 0 && charsToTrim.contains(sb.substring(0, 1))) {
            sb.deleteCharAt(0);
        }
        return sb.toString();
    }

    public static String rtrim(String value, String charsToTrim) {
        if (value == null || charsToTrim == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(value);
        while (sb.length() > 0
                && charsToTrim.contains(sb.substring(sb.length() - 1, sb.length()))) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public static String toUpperCase(String value) {
        if (value == null) {
            return null;
        }
        return value.toUpperCase();
    }

    public static String toLowerCase(String value) {
        if (value == null) {
            return null;
        }
        return value.toLowerCase();
    }

    public static boolean isOpenBracket(char c) {
        return c == '[' || c == '{' || c == '(';
    }

    public static boolean isCloseBracket(char c) {
        return c == ']' || c == '}' || c == ')';
    }

    /**
     * Converts a sequence to a string with truncation if it exceeds the maximum number of fields.
     * This is useful for limiting the size of string representations of large collections.
     *
     * @param lst the collection to convert to string
     * @param start the prefix string
     * @param sep the separator between elements
     * @param end the suffix string
     * @param maxFields the maximum number of fields to include before truncation
     * @return the truncated string representation
     */
    public static String truncatedString(
            Collection<?> lst, String start, String sep, String end, int maxFields) {
        boolean truncated = lst.size() > maxFields;
        int numFields = truncated ? Math.max(0, maxFields - 1) : lst.size();

        StringBuilder builder = new StringBuilder();
        builder.append(start);

        Iterator<?> iterator = lst.iterator();
        for (int i = 0; i < numFields; i++) {
            if (i > 0) {
                builder.append(sep);
            }
            builder.append(iterator.next());
        }

        if (truncated) {
            builder.append(sep)
                    .append("... ")
                    .append(lst.size() - numFields)
                    .append(" more fields");
        }

        builder.append(end);
        return builder.toString();
    }

    public static String truncatedString(Collection<?> lst, String start, String sep, String end) {
        return truncatedString(lst, start, sep, end, DEFAULT_MAX_FIELDS);
    }
}
