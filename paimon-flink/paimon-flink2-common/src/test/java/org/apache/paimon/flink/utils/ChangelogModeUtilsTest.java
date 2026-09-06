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

package org.apache.paimon.flink.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChangelogModeUtils}. */
public class ChangelogModeUtilsTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "2.1.0",
                "2.2.0",
                "3.0.0",
                "2.1.0-amzn-0",
                "2.1-SNAPSHOT",
                "2.2-SNAPSHOT",
                "2.10-SNAPSHOT"
            })
    void testVersionAtLeast21(String version) {
        assertThat(ChangelogModeUtils.isVersionAtLeast21(version)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "2.0.0",
                "2.0-SNAPSHOT",
                "2.0-vvr-11.2-SNAPSHOT",
                "2.0-rc1",
                "1.20.1",
                "1.20-SNAPSHOT",
                "1.20-vvr-11.2-SNAPSHOT",
                "<unknown>",
                "",
                "2",
                "not-a-version"
            })
    void testVersionBelow21(String version) {
        assertThat(ChangelogModeUtils.isVersionAtLeast21(version)).isFalse();
    }
}
