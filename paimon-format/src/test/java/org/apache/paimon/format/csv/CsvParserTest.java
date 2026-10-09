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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests CSV parsing independently of the writer that produces escaped fields. */
class CsvParserTest {

    private static final RowType ROW_TYPE =
            DataTypes.ROW(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"userid\":\"1\",\"memo\":\"a\\nb\"}",
                "{\"userid\":\"2\",\"memo\":\"C:\\\\Users\\\\test\"}",
                "{\"userid\":\"3\",\"memo\":\"a\\tb\"}",
                "{\"userid\":\"4\",\"memo\":\"a\\\"b\"}",
                "{\"userid\":\"5\",\"memo\":\"\\u871c\\u96ea\"}",
                "{\"userid\":\"6\",\"memo\":\"\"}",
                "unquoted\\path\\",
                "\\\\server\\path",
                "embedded\"quote",
                "doubled\"\"quotes",
                "leading \\\"quote",
                "  \"value\"",
                "\\\"leading"
            })
    void testUnquotedFieldsPreserveQuotesAndEscapes(String value) {
        CsvParser parser = parser("\t", "\"", "\\");
        // Exercise each column and reuse the parser across rows.
        assertThat(parser.parse(value + "\tsecond\tlast")).isEqualTo(row(value, "second", "last"));
        assertThat(parser.parse("first\t" + value + "\tlast"))
                .isEqualTo(row("first", value, "last"));
        assertThat(parser.parse("first\tsecond\t" + value))
                .isEqualTo(row("first", "second", value));
    }

    @Test
    void testQuotedFieldsDecodeEscapesAndContainDelimiters() {
        CsvParser parser = parser(",", "\"", "\\");
        assertThat(parser.parse("\"a,b\",\"C:\\\\Users\\\\test\",\"say \\\"hello\\\"\""))
                .isEqualTo(row("a,b", "C:\\Users\\test", "say \"hello\""));
        // Preserve Paimon's existing doubled-quote behavior; Spark's default differs here.
        assertThat(parser.parse("\"a\"\"b\",unquoted\\path,\"c\\\\d\""))
                .isEqualTo(row("a\"b", "unquoted\\path", "c\\d"));
    }

    @Test
    void testUnknownEscapesInQuotedFieldsArePreserved() {
        assertThat(parser(",", "\"", "\\").parse("\"a\\nb\",\"\\u871c\",\"path\\q\""))
                .isEqualTo(row("a\\nb", "\\u871c", "path\\q"));
    }

    @Test
    void testEmptyQuotedFieldsResetQuotingState() {
        CsvParser parser = parser(",", "\"", "\\");
        assertThat(parser.parse("\"\",plain\\value,\"\"")).isEqualTo(row("", "plain\\value", ""));
        assertThat(parser.parse("\"first\",,last\\value"))
                .isEqualTo(GenericRow.of(fromString("first"), null, fromString("last\\value")));
    }

    @Test
    void testEmbeddedQuoteDoesNotProtectDelimiter() {
        assertThat(parser(",", "\"", "\\").parse("a\"b,c\"d,last"))
                .isEqualTo(row("a\"b", "c\"d", "last"));
    }

    @Test
    void testCustomQuoteAndEscapeCharacters() {
        CsvParser parser = parser("|", "'", "/");
        assertThat(parser.parse("raw//path/'quote|'a/'b//c'|'x|y'"))
                .isEqualTo(row("raw//path/'quote", "a'b/c", "x|y"));
    }

    @Test
    void testDisabledQuotingAndEscapingPreserveNullCharacters() {
        assertThat(parser(",", "\0", "\0").parse("a\0b,\0start,end\0"))
                .isEqualTo(row("a\0b", "\0start", "end\0"));
    }

    @Test
    void testUnterminatedQuotedFieldRemainsNull() {
        assertThat(parser(",", "\"", "\\").parse("first,second,\"unterminated"))
                .isEqualTo(GenericRow.of(fromString("first"), fromString("second"), null));
    }

    @Test
    void testQuotedNullLiteralWithProjectionAndParserReuse() {
        Options options = new Options();
        options.set(CsvOptions.NULL_LITERAL, "NULL");
        CsvParser parser = new CsvParser(ROW_TYPE, new int[] {1, 0, 2}, new CsvOptions(options));
        assertThat(parser.parse("NULL,\"NULL\",\"\""))
                .isEqualTo(GenericRow.of(fromString("NULL"), null, fromString("")));
        assertThat(parser.parse("\"NULL\",NULL,plain\\value"))
                .isEqualTo(GenericRow.of(null, fromString("NULL"), fromString("plain\\value")));
    }

    private static CsvParser parser(String delimiter, String quote, String escape) {
        Options options = new Options();
        options.set(CsvOptions.FIELD_DELIMITER, delimiter);
        options.set(CsvOptions.QUOTE_CHARACTER, quote);
        options.set(CsvOptions.ESCAPE_CHARACTER, escape);
        return new CsvParser(ROW_TYPE, new int[] {0, 1, 2}, new CsvOptions(options));
    }

    private static GenericRow row(String first, String second, String third) {
        return GenericRow.of(fromString(first), fromString(second), fromString(third));
    }
}
