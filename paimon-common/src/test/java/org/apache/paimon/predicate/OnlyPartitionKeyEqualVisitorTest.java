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

package org.apache.paimon.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link OnlyPartitionKeyEqualVisitor}. */
public class OnlyPartitionKeyEqualVisitorTest {

    @Test
    public void testContradictoryPartitionKeysNotDroppable() {
        OnlyPartitionKeyEqualVisitor visitor =
                new OnlyPartitionKeyEqualVisitor(Arrays.asList("pt", "dt"));

        // pt = 'a' AND pt = 'b': no partition matches; must not be treated as a
        // partition drop (the old last-write-wins map made it drop partition pt='b').
        Predicate p1 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        0,
                        "pt",
                        Collections.singletonList(BinaryString.fromString("a")));
        Predicate p2 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        0,
                        "pt",
                        Collections.singletonList(BinaryString.fromString("b")));
        Predicate contradiction = PredicateBuilder.and(Arrays.asList(p1, p2));
        assertThat(contradiction.visit(visitor)).isFalse();
    }

    @Test
    public void testDistinctPartitionKeysDroppable() {
        OnlyPartitionKeyEqualVisitor visitor =
                new OnlyPartitionKeyEqualVisitor(Arrays.asList("pt", "dt"));
        Predicate p1 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        0,
                        "pt",
                        Collections.singletonList(BinaryString.fromString("a")));
        Predicate p2 =
                new LeafPredicate(
                        Equal.INSTANCE,
                        DataTypes.STRING(),
                        1,
                        "dt",
                        Collections.singletonList(BinaryString.fromString("2024")));
        Predicate combined = PredicateBuilder.and(Arrays.asList(p1, p2));
        assertThat(combined.visit(visitor)).isTrue();
        assertThat(visitor.partitions()).containsEntry("pt", "a").containsEntry("dt", "2024");
    }

    @Test
    public void testNonPartitionKeyNotDroppable() {
        OnlyPartitionKeyEqualVisitor visitor =
                new OnlyPartitionKeyEqualVisitor(Collections.singletonList("pt"));
        Predicate px =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 2, "x", Collections.singletonList(1));
        assertThat(px.visit(visitor)).isFalse();
        assertThat(visitor.partitions()).isEmpty();
    }
}
