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

package org.apache.paimon.format.json;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import static org.apache.paimon.options.description.TextElement.text;

/** Options for Json format. */
public class JsonOptions {

    public static final ConfigOption<Boolean> JSON_IGNORE_PARSE_ERRORS =
            ConfigOptions.key("json.ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore parse errors for JSON format");

    public static final ConfigOption<MapNullKeyMode> JSON_MAP_NULL_KEY_MODE =
            ConfigOptions.key("json.map-null-key-mode")
                    .enumType(MapNullKeyMode.class)
                    .defaultValue(MapNullKeyMode.FAIL)
                    .withDescription("How to handle map keys that are null.");

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("json.map-null-key-literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Literal to use for null map keys when map-null-key-mode is LITERAL");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("json.line-delimiter")
                    .stringType()
                    .defaultValue("\n")
                    .withDescription("The line delimiter for JSON format");

    /** Enum for handling null keys in JSON maps. */
    public enum MapNullKeyMode implements DescribedEnum {
        FAIL("fail", "Throw an exception when encountering null map keys."),
        DROP("drop", "Drop entries with null keys from the map."),
        LITERAL("literal", "Replace null keys with a literal string value.");

        private final String value;
        private final String description;

        MapNullKeyMode(String value, String description) {
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

    private final boolean ignoreParseErrors;
    private final MapNullKeyMode mapNullKeyMode;
    private final String mapNullKeyLiteral;
    private final String lineDelimiter;

    public JsonOptions(Options options) {
        this.ignoreParseErrors = options.get(JSON_IGNORE_PARSE_ERRORS);
        this.mapNullKeyMode = options.get(JSON_MAP_NULL_KEY_MODE);
        this.mapNullKeyLiteral = options.get(JSON_MAP_NULL_KEY_LITERAL);
        this.lineDelimiter = options.get(LINE_DELIMITER);
    }

    public boolean ignoreParseErrors() {
        return ignoreParseErrors;
    }

    public MapNullKeyMode getMapNullKeyMode() {
        return mapNullKeyMode;
    }

    public String getMapNullKeyLiteral() {
        return mapNullKeyLiteral;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }
}
