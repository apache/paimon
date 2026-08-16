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

package org.apache.paimon.iceberg;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergHiveMetadataCommitter}. */
class IcebergHiveMetadataCommitterTest {

    @Test
    void testNormalizeColumnComment() {
        assertThat(IcebergHiveMetadataCommitter.normalizeColumnComment(null)).isNull();

        String maxLengthComment = repeat('a', 255);
        assertThat(IcebergHiveMetadataCommitter.normalizeColumnComment(maxLengthComment))
                .isEqualTo(maxLengthComment);

        String longComment = repeat('b', 256);
        assertThat(IcebergHiveMetadataCommitter.normalizeColumnComment(longComment))
                .hasSize(255)
                .isEqualTo(repeat('b', 252) + "...");

        assertThat(IcebergHiveMetadataCommitter.normalizeColumnComment("line1\nline2\r\nline3"))
                .isEqualTo("line1 line2  line3");

        assertThat(
                        IcebergHiveMetadataCommitter.normalizeColumnComment(
                                "first\n" + repeat('c', 253)))
                .hasSize(255)
                .endsWith("...");
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }
}
