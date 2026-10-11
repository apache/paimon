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

package org.apache.paimon.flink.action.cdc.mysql;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source type name is case-folded before it is matched against the names this class knows. Under
 * a Turkish default locale 'i' maps to a dotted capital, so those conversions must pin {@link
 * Locale#ROOT} or a type whose name contains an 'i' stops being recognized.
 */
class TurkishLocaleTypeNameTest {

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
    void lowerCaseTypeNameKeepsItsShortType() {
        // a locale-sensitive uppercase turns "int" into "İNT", which no case arm names
        assertThat(MySqlTypeUtils.getTypeInfo("int").f0).isEqualTo("INT");
    }

    @Test
    void lowerCaseGeoTypeIsStillRecognized() {
        assertThat(MySqlTypeUtils.isGeoType("point")).isTrue();
        assertThat(MySqlTypeUtils.isGeoType("linestring")).isTrue();
    }
}
