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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import static org.apache.paimon.options.description.TextElement.text;

/** Options for csv format. */
public class CsvOptions {

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("csv.field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withFallbackKeys("field-delimiter", "seq")
                    .withDescription("The field delimiter for CSV or TXT format");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("csv.line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withFallbackKeys("lineSep")
                    .withDescription("The line delimiter for CSV format");

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key("csv.quote-character")
                    .stringType()
                    .defaultValue("\"")
                    .withFallbackKeys("quote")
                    .withDescription("The quote character for CSV format");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key("csv.escape-character")
                    .stringType()
                    .defaultValue("\\")
                    .withFallbackKeys("escape")
                    .withDescription("The escape character for CSV format");

    public static final ConfigOption<Boolean> INCLUDE_HEADER =
            ConfigOptions.key("csv.include-header")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("header")
                    .withDescription("Whether to include header in CSV files");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key("csv.null-literal")
                    .stringType()
                    .defaultValue("")
                    .withFallbackKeys("nullvalue")
                    .withDescription("The literal for null values in CSV format");

    public static final ConfigOption<Mode> MODE =
            ConfigOptions.key("csv.mode")
                    .enumType(Mode.class)
                    .defaultValue(Mode.PERMISSIVE)
                    .withFallbackKeys("mode")
                    .withDescription(
                            "Allows a mode for dealing with corrupt records during reading.");

    private final String fieldDelimiter;
    private final String lineDelimiter;
    private final String nullLiteral;
    private final boolean includeHeader;
    private final String quoteCharacter;
    private final String escapeCharacter;
    private final Mode mode;

    public CsvOptions(Options options) {
        this.fieldDelimiter = options.get(FIELD_DELIMITER);
        this.lineDelimiter = options.get(LINE_DELIMITER);
        this.nullLiteral = options.get(NULL_LITERAL);
        this.includeHeader = options.get(INCLUDE_HEADER);
        this.quoteCharacter = options.get(QUOTE_CHARACTER);
        this.escapeCharacter = options.get(ESCAPE_CHARACTER);
        this.mode = options.get(MODE);
    }

    public String fieldDelimiter() {
        return fieldDelimiter;
    }

    public String lineDelimiter() {
        return lineDelimiter;
    }

    public String nullLiteral() {
        return nullLiteral;
    }

    public boolean includeHeader() {
        return includeHeader;
    }

    public String quoteCharacter() {
        return quoteCharacter;
    }

    public String escapeCharacter() {
        return escapeCharacter;
    }

    public Mode mode() {
        return mode;
    }

    /** Mode for dealing with corrupt records during reading. */
    public enum Mode implements DescribedEnum {
        PERMISSIVE("permissive", "Sets malformed fields to null."),
        DROPMALFORMED("dropmalformed", "Ignores the whole corrupted records."),
        FAILFAST("failfast", "Throws an exception when it meets corrupted records.");

        private final String value;
        private final String description;

        Mode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }

        public String getValue() {
            return value;
        }
    }
}
