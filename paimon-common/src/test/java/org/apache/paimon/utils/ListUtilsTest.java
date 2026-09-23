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

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ListUtils}. */
public class ListUtilsTest {

    @Test
    public void testTryReplace() {
        assertThat(
                        ListUtils.tryReplace(
                                Arrays.asList("a", "b", "c", "d", "e", "f"),
                                Arrays.asList("b", "c", "d"),
                                Arrays.asList("n", "w")))
                .containsExactly("a", "n", "w", "e", "f");

        assertThat(
                        ListUtils.tryReplace(
                                Arrays.asList("b", "c", "d"),
                                Arrays.asList("b", "c", "d"),
                                Collections.singletonList("n")))
                .containsExactly("n");

        assertThat(
                        ListUtils.tryReplace(
                                Arrays.asList("a", "b", "c"),
                                Collections.singletonList("b"),
                                Collections.emptyList()))
                .containsExactly("a", "c");
    }

    @Test
    public void testTryReplaceRequiresContinuousMatch() {
        assertThat(
                        ListUtils.tryReplace(
                                Arrays.asList("a", "b", "x", "c", "d"),
                                Arrays.asList("b", "c", "d"),
                                Collections.singletonList("n")))
                .isNull();
    }

    @Test
    public void testTryReplaceRequiresSameOrder() {
        assertThat(
                        ListUtils.tryReplace(
                                Arrays.asList("a", "d", "c", "b", "e"),
                                Arrays.asList("b", "c", "d"),
                                Collections.singletonList("n")))
                .isNull();
    }

    @Test
    public void testTryReplaceRequiresNonEmptyReplacedList() {
        assertThatThrownBy(
                        () ->
                                ListUtils.tryReplace(
                                        Collections.singletonList("a"),
                                        Collections.emptyList(),
                                        Collections.singletonList("n")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot replace an empty list");
    }
}
