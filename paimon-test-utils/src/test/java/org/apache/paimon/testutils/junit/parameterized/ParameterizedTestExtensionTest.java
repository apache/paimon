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

package org.apache.paimon.testutils.junit.parameterized;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ParameterizedTestExtension} can reach the {@link Parameters} provider and the
 * {@link Parameter} fields even when Java access control denies the extension access to them.
 *
 * <p>Both nested classes below are picked up by surefire on their own, because the unit test
 * pattern {@code **}{@code /*Test.*} also matches nested class files.
 */
class ParameterizedTestExtensionTest {

    private static final List<String> VALUES = Arrays.asList("a", "b");

    /** The {@code @Parameters} provider is not accessible to the extension. */
    @ExtendWith(ParameterizedTestExtension.class)
    static class InaccessibleParameterProviderTest {

        @Parameter public String value;

        @Parameters(name = "value = {0}")
        private static List<String> parameters() {
            return VALUES;
        }

        @TestTemplate
        void injectsValueFromInaccessibleProvider() {
            assertThat(VALUES).contains(value);
        }
    }

    /** The {@code @Parameter} field is not accessible to the extension. */
    @ExtendWith(ParameterizedTestExtension.class)
    static class InaccessibleParameterFieldTest {

        @Parameter private String value;

        @Parameters(name = "value = {0}")
        public static List<String> parameters() {
            return VALUES;
        }

        @TestTemplate
        void injectsValueIntoInaccessibleField() {
            assertThat(VALUES).contains(value);
        }
    }
}
