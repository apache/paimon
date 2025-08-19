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

package org.apache.paimon.options;

/** Options for csv format. */
public class FormatOptions {
    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("The field delimiter for CSV or TXT format");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withDescription("The line delimiter for CSV format");

    public static final ConfigOption<String> QUOTE_CHARACTER =
            ConfigOptions.key("quote-character")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription("The quote character for CSV format");

    public static final ConfigOption<String> ESCAPE_CHARACTER =
            ConfigOptions.key("escape-character")
                    .stringType()
                    .defaultValue("\\")
                    .withDescription("The escape character for CSV format");

    public static final ConfigOption<Boolean> INCLUDE_HEADER =
            ConfigOptions.key("include-header")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to include header in CSV files");

    public static final ConfigOption<String> NULL_LITERAL =
            ConfigOptions.key("null-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription("The literal for null values in CSV format");
}
