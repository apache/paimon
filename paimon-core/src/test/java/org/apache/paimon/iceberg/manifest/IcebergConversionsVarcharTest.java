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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergConversionsVarcharTest {

    @Test
    void testEmptyString() {
        String empty = "";
        ByteBuffer result = IcebergConversions.toByteBuffer(DataTypes.VARCHAR(10), empty);
        String decodedString = new String(result.array(), StandardCharsets.UTF_8);
        assertThat(result.array()).isEmpty();
        assertThat(empty).isEqualTo(decodedString);
    }

    @Test
    void testNullHandling() {
        assertThatThrownBy(() -> IcebergConversions.toByteBuffer(DataTypes.VARCHAR(10), null))
                .isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("provideSpecialStrings")
    @DisplayName("Test special string cases")
    void testSpecialStrings(String input) {
        ByteBuffer result = IcebergConversions.toByteBuffer(DataTypes.VARCHAR(100), input);
        String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
        assertThat(decoded).isEqualTo(input);
    }

    private static Stream<Arguments> provideSpecialStrings() {
        return Stream.of(
                Arguments.of("Hello\u0000World"), // Embedded null
                Arguments.of("\n\r\t"), // Control characters
                Arguments.of(" "), // Single space
                Arguments.of("    "), // Multiple spaces
                Arguments.of("①②③"), // Unicode numbers
                Arguments.of("🌟🌞🌝"), // Emojis
                Arguments.of("Hello\uD83D\uDE00World"), // Surrogate pairs
                Arguments.of("\uFEFF"), // Byte Order Mark
                Arguments.of("Hello\\World"), // Backslashes
                Arguments.of("Hello\"World"), // Quotes
                Arguments.of("Hello'World"), // Single quotes
                Arguments.of("Hello\bWorld"), // Backspace
                Arguments.of("Hello\fWorld") // Form feed
                );
    }

    @ParameterizedTest
    @MethodSource("provideLongStrings")
    void testLongStrings(String input) {
        ByteBuffer result =
                IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length()), input);
        String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
        assertThat(decoded).isEqualTo(input).hasSize(input.length());
    }

    private static Stream<Arguments> provideLongStrings() {
        return Stream.of(
                Arguments.of(createString(1)),
                Arguments.of(createString(10)),
                Arguments.of(createString(100)),
                Arguments.of(createString(1000)),
                Arguments.of(createString(10000)));
    }

    private static String createString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @Test
    void testMultiByteCharacters() {
        String[] inputs = {
            "中文", // Chinese
            "한글", // Korean
            "日本語", // Japanese
            "🌟", // Emoji (4 bytes)
            "Café", // Latin-1 Supplement
            "Привет", // Cyrillic
            "שָׁלוֹם", // Hebrew with combining marks
            "ᄀᄁᄂᄃᄄ", // Hangul Jamo
            "बहुत बढ़िया", // Devanagari
            "العربية" // Arabic
        };

        for (String input : inputs) {
            ByteBuffer result =
                    IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length() * 4), input);
            String decoded = new String(result.array(), 0, result.limit(), StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo(input);
            assertThat(result.limit()).isGreaterThanOrEqualTo(input.length());
        }
    }

    @Test
    void testBufferProperties() {
        String input = "Hello, World!";
        ByteBuffer result =
                IcebergConversions.toByteBuffer(DataTypes.VARCHAR(input.length()), input);

        assertThat(result.limit()).isEqualTo(result.array().length);
        assertThat(containsTrailingZeros(result)).isFalse();
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        String[] inputs = new String[threadCount];
        ByteBuffer[] results = new ByteBuffer[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            inputs[index] = "Thread" + index;
            threads[index] =
                    new Thread(
                            () -> {
                                results[index] =
                                        IcebergConversions.toByteBuffer(
                                                DataTypes.VARCHAR(inputs[index].length()),
                                                inputs[index]);
                            });
            threads[index].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        for (int i = 0; i < threadCount; i++) {
            String decoded =
                    new String(results[i].array(), 0, results[i].limit(), StandardCharsets.UTF_8);
            assertThat(decoded).isEqualTo(inputs[i]);
        }
    }

    private boolean containsTrailingZeros(ByteBuffer buffer) {
        byte[] array = buffer.array();
        for (int i = buffer.limit(); i < array.length; i++) {
            if (array[i] != 0) {
                return true;
            }
        }
        return false;
    }
}
