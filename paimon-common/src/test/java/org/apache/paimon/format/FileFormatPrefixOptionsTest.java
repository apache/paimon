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

package org.apache.paimon.format;

import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the format-prefixed option filtering in {@link FileFormat}. */
class FileFormatPrefixOptionsTest {

    /** U+0130, whose lower case under {@link Locale#ROOT} is the two characters below. */
    private static final String DOTTED_CAPITAL_I = "İ";

    private static final String LOWER_CASE_DOTTED_I = "i̇";

    @Test
    void prefixIsMatchedCaseInsensitively() {
        Options options = new Options();
        options.set("ORC.compression", "zstd");
        options.set("avro.codec", "snappy");

        Options prefixed = new TestFileFormat("orc").getIdentifierPrefixOptions(options);

        assertThat(prefixed.toMap()).containsExactly(entry("orc.compression", "zstd"));
    }

    @Test
    void anIdentifierThatLowerCasesLongerKeepsItsOptions() {
        assertThat(DOTTED_CAPITAL_I.toLowerCase(Locale.ROOT)).isEqualTo(LOWER_CASE_DOTTED_I);

        Options options = new Options();
        options.set(DOTTED_CAPITAL_I + ".compression", "zstd");

        Options prefixed = new TestFileFormat(DOTTED_CAPITAL_I).getIdentifierPrefixOptions(options);

        // slicing the key at the length of the lower-cased prefix would drop the "c"
        assertThat(prefixed.toMap())
                .containsExactly(entry(LOWER_CASE_DOTTED_I + ".compression", "zstd"));
    }

    @Test
    void aKeyThatIsOnlyThePrefixYieldsAnEmptySuffix() {
        Options options = new Options();
        options.set(DOTTED_CAPITAL_I + ".", "zstd");

        Options prefixed = new TestFileFormat(DOTTED_CAPITAL_I).getIdentifierPrefixOptions(options);

        // this key is shorter than the lower-cased prefix, so slicing at its length overruns it
        assertThat(prefixed.toMap()).containsExactly(entry(LOWER_CASE_DOTTED_I + ".", "zstd"));
    }

    private static class TestFileFormat extends FileFormat {

        private TestFileFormat(String formatIdentifier) {
            super(formatIdentifier);
        }

        @Override
        public FormatReaderFactory createReaderFactory(
                RowType dataSchemaRowType,
                RowType projectedRowType,
                @Nullable List<Predicate> filters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatWriterFactory createWriterFactory(RowType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void validateDataFields(RowType rowType) {}
    }
}
