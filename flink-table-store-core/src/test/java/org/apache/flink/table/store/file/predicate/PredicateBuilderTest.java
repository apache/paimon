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

package org.apache.flink.table.store.file.predicate;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PredicateBuilder}. */
public class PredicateBuilderTest {

    @Test
    public void testSplitAnd() {
        Predicate child1 =
                PredicateBuilder.or(
                        PredicateBuilder.isNull(0),
                        PredicateBuilder.isNull(1),
                        PredicateBuilder.isNull(2));
        Predicate child2 =
                PredicateBuilder.and(
                        PredicateBuilder.isNull(3),
                        PredicateBuilder.isNull(4),
                        PredicateBuilder.isNull(5));
        Predicate child3 = PredicateBuilder.isNull(6);

        assertThat(PredicateBuilder.splitAnd(PredicateBuilder.and(child1, child2, child3)))
                .isEqualTo(
                        Arrays.asList(
                                child1,
                                PredicateBuilder.isNull(3),
                                PredicateBuilder.isNull(4),
                                PredicateBuilder.isNull(5),
                                child3));
    }
}
