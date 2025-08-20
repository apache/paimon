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

/** Options for csv format. */
public class CsvOptions {

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("csv.field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withFallbackKeys("field-delimiter")
                    .withDescription("The field delimiter for CSV or TXT format");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("csv.line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withDescription("The line delimiter for CSV format");

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key("csv.quote-character")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription("The quote character for CSV format");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key("csv.escape-character")
                    .stringType()
                    .defaultValue("\\")
                    .withDescription("The escape character for CSV format");

    public static final ConfigOption<Boolean> INCLUDE_HEADER =
            ConfigOptions.key("csv.include-header")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to include header in CSV files");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key("csv.null-literal")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The literal for null values in CSV format");

    private final String fieldDelimiter;
    private final String lineDelimiter;
    private final String nullLiteral;
    private final boolean includeHeader;
    private final String quoteCharacter;
    private final String escapeCharacter;

    public CsvOptions(Options options) {
        this.fieldDelimiter = options.get(FIELD_DELIMITER);
        this.lineDelimiter = options.get(LINE_DELIMITER);
        this.nullLiteral = options.get(NULL_LITERAL);
        this.includeHeader = options.get(INCLUDE_HEADER);
        this.quoteCharacter = options.get(QUOTE_CHARACTER);
        this.escapeCharacter = options.get(ESCAPE_CHARACTER);
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
}
