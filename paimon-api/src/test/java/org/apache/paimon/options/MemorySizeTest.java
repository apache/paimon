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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MemorySize}. */
class MemorySizeTest {

    @ParameterizedTest
    @ValueSource(strings = {"0 bytes", "123 bytes", "1 kb", "2 mb", "3 gb"})
    void testLocaleIndependentRoundTrip(String value) {
        Locale originalLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.Category.FORMAT, new Locale("ar", "EG"));
            MemorySize size = MemorySize.parse(value);
            assertThat(size.toString()).isEqualTo(value);

            ConfigOption<MemorySize> option =
                    ConfigOptions.key("memory-size").memoryType().noDefaultValue();
            Options options = new Options();
            options.set(option, size);
            assertThat(options.get(option)).isEqualTo(size);
        } finally {
            Locale.setDefault(Locale.Category.FORMAT, originalLocale);
        }
    }
}
