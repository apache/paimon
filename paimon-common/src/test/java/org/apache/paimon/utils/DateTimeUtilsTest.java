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

package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DateTimeUtils}. */
public class DateTimeUtilsTest {

    @Test
    public void testFormatLocalDateTime() {
        LocalDateTime time = LocalDateTime.of(2023, 8, 30, 12, 30, 59, 999_999_999);
        String[] expectations = new String[10];
        expectations[0] = "2023-08-30 12:30:59";
        expectations[1] = "2023-08-30 12:30:59.9";
        for (int i = 2; i <= 9; i++) {
            expectations[i] = expectations[i - 1] + "9";
        }

        for (int precision = 0; precision <= 9; precision++) {
            assertThat(DateTimeUtils.formatLocalDateTime(time, precision))
                    .isEqualTo(expectations[precision]);
        }
    }
}
