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

package org.apache.paimon.tools.ci.spotless;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * These tests verify that the regex patterns correctly replace illegal imports with their shaded
 * versions as configured in the Spotless Maven plugin.
 */
class SpotlessRegexReplacementTest {

    // Regex patterns from pom.xml spotless configuration
    private static final String GUAVA_SEARCH_REGEX = "import com\\.google\\.common\\.([^;]+);";
    private static final String GUAVA_REPLACEMENT =
            "import org.apache.paimon.shade.guava30.com.google.common.$1;";

    private static final String JACKSON_SEARCH_REGEX =
            "import com\\.fasterxml\\.jackson\\.([^;]+);";
    private static final String JACKSON_REPLACEMENT =
            "import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.$1;";

    private static final Pattern GUAVA_PATTERN = Pattern.compile(GUAVA_SEARCH_REGEX);
    private static final Pattern JACKSON_PATTERN = Pattern.compile(JACKSON_SEARCH_REGEX);

    @ParameterizedTest
    @CsvSource({
        "'import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;', 'import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;'",
        "'import org.apache.paimon.shade.guava30.com.google.common.base.Strings;', 'import org.apache.paimon.shade.guava30.com.google.common.base.Strings;'",
        "'import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ListenableFuture;', 'import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ListenableFuture;'",
        "'import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;', 'import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;'"
    })
    void testGuavaImportReplacementVariations(String input, String expected) {
        String result = GUAVA_PATTERN.matcher(input).replaceAll(GUAVA_REPLACEMENT);
        assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;', 'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;'",
        "'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;', 'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonParser;'",
        "'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;', 'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;'",
        "'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;', 'import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;'"
    })
    void testJacksonImportReplacementVariations(String input, String expected) {
        String result = JACKSON_PATTERN.matcher(input).replaceAll(JACKSON_REPLACEMENT);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void testNonMatchingImportsAreNotChanged() {
        String[] nonMatchingImports = {
            "import java.util.List;",
            "import org.apache.paimon.data.Row;",
            "import org.slf4j.Logger;",
            "import javax.annotation.Nullable;",
            "import com.google.protobuf.Message;",
            "import com.fasterxml.jackson2.databind.ObjectMapper;",
            "import static com.google.common.collect.Lists.newArrayList;",
            "// import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;"
        };

        for (String importStatement : nonMatchingImports) {
            String guavaResult =
                    GUAVA_PATTERN.matcher(importStatement).replaceAll(GUAVA_REPLACEMENT);
            String jacksonResult =
                    JACKSON_PATTERN.matcher(importStatement).replaceAll(JACKSON_REPLACEMENT);

            assertThat(guavaResult)
                    .as("Guava pattern should not match: %s", importStatement)
                    .isEqualTo(importStatement);
            assertThat(jacksonResult)
                    .as("Jackson pattern should not match: %s", importStatement)
                    .isEqualTo(importStatement);
        }
    }
}
