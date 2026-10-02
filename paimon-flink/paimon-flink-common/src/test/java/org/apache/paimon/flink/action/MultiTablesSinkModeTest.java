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

package org.apache.paimon.flink.action;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mode name is case-folded before it is matched, so the conversion has to pin {@link
 * Locale#ROOT} or a name containing an 'i' stops matching under a Turkish default.
 */
class MultiTablesSinkModeTest {

    private Locale original;

    @BeforeEach
    void setUp() {
        original = Locale.getDefault();
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterEach
    void tearDown() {
        Locale.setDefault(original);
    }

    @Test
    void upperCaseModeNameStillParses() {
        // DIVIDED contains an 'I', which a locale-sensitive lowercase turns into a dotless one
        assertThat(MultiTablesSinkMode.fromString("DIVIDED"))
                .isEqualTo(MultiTablesSinkMode.DIVIDED);
        assertThat(MultiTablesSinkMode.fromString("divided"))
                .isEqualTo(MultiTablesSinkMode.DIVIDED);
    }

    @Test
    void configStringRoundTrips() {
        for (MultiTablesSinkMode mode : MultiTablesSinkMode.values()) {
            assertThat(MultiTablesSinkMode.fromString(mode.configString())).isEqualTo(mode);
        }
    }
}
