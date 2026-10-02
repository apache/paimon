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

package org.apache.paimon.hive.clone;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the format-prefixed option filtering in {@link HiveTableCloneExtractor}. */
class HiveTableCloneExtractorTest {

    /** U+0130, whose lower case under {@link Locale#ROOT} is the two characters below. */
    private static final String DOTTED_CAPITAL_I = "İ";

    private static final String LOWER_CASE_DOTTED_I = "i̇";

    @Test
    void prefixIsMatchedCaseInsensitively() {
        Map<String, String> options = new HashMap<>();
        options.put("ORC.compression", "zstd");
        options.put("avro.codec", "snappy");

        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions("orc", options))
                .containsExactly(entry("orc.compression", "zstd"));
    }

    @Test
    void anIdentifierThatLowerCasesLongerKeepsItsOptions() {
        assertThat(DOTTED_CAPITAL_I.toLowerCase(Locale.ROOT)).isEqualTo(LOWER_CASE_DOTTED_I);

        Map<String, String> options = new HashMap<>();
        options.put(DOTTED_CAPITAL_I + ".compression", "zstd");

        // slicing the key at the length of the lower-cased prefix would drop the "c"
        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions(DOTTED_CAPITAL_I, options))
                .containsExactly(entry(LOWER_CASE_DOTTED_I + ".compression", "zstd"));
    }

    @Test
    void aKeyThatIsOnlyThePrefixYieldsAnEmptySuffix() {
        Map<String, String> options = new HashMap<>();
        options.put(DOTTED_CAPITAL_I + ".", "zstd");

        // this key is shorter than the lower-cased prefix, so slicing at its length overruns it
        assertThat(HiveTableCloneExtractor.getIdentifierPrefixOptions(DOTTED_CAPITAL_I, options))
                .containsExactly(entry(LOWER_CASE_DOTTED_I + ".", "zstd"));
    }
}
