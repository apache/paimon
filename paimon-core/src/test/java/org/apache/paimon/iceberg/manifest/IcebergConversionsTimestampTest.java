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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergConversionsTimestampTest {

    @ParameterizedTest
    @MethodSource("provideTimestampConversionCases")
    void testTimestampToByteBuffer(int precision, Timestamp input, long expectedMicros) {
        ByteBuffer buffer = IcebergConversions.toByteBuffer(DataTypes.TIMESTAMP(precision), input);
        assertThat(buffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
        assertThat(buffer.getLong(0)).isEqualTo(expectedMicros);
    }

    private static Stream<Arguments> provideTimestampConversionCases() {
        Timestamp tsMillis =
                Timestamp.fromEpochMillis(1682164983524L); // 2023-04-22T13:03:03.524 (p=3)
        Timestamp tsMicros = Timestamp.fromMicros(1683849603123456L); // 2023-05-12T00:00:03.123456

        return Stream.of(
                // For p=3..6 we encode microseconds per Iceberg spec
                Arguments.of(3, tsMillis, 1682164983524000L), // micros from millis
                Arguments.of(4, tsMillis, 1682164983524000L),
                Arguments.of(5, tsMillis, 1682164983524000L),
                Arguments.of(6, tsMillis, 1682164983524000L),
                Arguments.of(6, tsMicros, 1683849603123456L)); // passthrough
    }

    @ParameterizedTest
    @MethodSource("provideInvalidPrecisions")
    @DisplayName("Invalid timestamp precisions for ByteBuffer conversion")
    void testTimestampToByteBufferInvalidPrecisions(int precision) {
        Timestamp timestamp = Timestamp.fromEpochMillis(1682164983524L);

        assertThatThrownBy(
                        () ->
                                IcebergConversions.toByteBuffer(
                                        DataTypes.TIMESTAMP(precision), timestamp))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
    }

    private static Stream<Arguments> provideInvalidPrecisions() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(7),
                Arguments.of(8),
                Arguments.of(9));
    }

    // ------------------------------------------------------------------------
    //  toPaimonObject tests
    // ------------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("provideTimestampToPaimonCases")
    void testToPaimonObjectForTimestamp(int precision, long serializedMicros, String expectedTs) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(serializedMicros);

        Timestamp actual =
                (Timestamp)
                        IcebergConversions.toPaimonObject(DataTypes.TIMESTAMP(precision), bytes);

        assertThat(actual.toString()).isEqualTo(expectedTs);
    }

    private static Stream<Arguments> provideTimestampToPaimonCases() {
        return Stream.of(
                // Provide binary in micros; p=3..6 should all parse as micros
                Arguments.of(3, -1356022717123L, "1927-01-12T07:01:22.877"),
                Arguments.of(3, 1713790983524L, "2024-04-22T13:03:03.524"),
                Arguments.of(6, 1640690931207203L, "2021-12-28T11:28:51.207203"));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidTimestampCases")
    void testToPaimonObjectTimestampInvalid(int precision, long serializedMicros) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(serializedMicros);

        assertThatThrownBy(
                        () ->
                                IcebergConversions.toPaimonObject(
                                        DataTypes.TIMESTAMP(precision), bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
    }

    private static Stream<Arguments> provideInvalidTimestampCases() {
        return Stream.of(Arguments.of(0, 1698686153L), Arguments.of(9, 1698686153123456789L));
    }
}
