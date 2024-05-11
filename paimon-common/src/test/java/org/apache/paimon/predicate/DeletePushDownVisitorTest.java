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

import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** DeletePushDownVisitorTest tests the DeletePushDownVisitor. */
public class DeletePushDownVisitorTest {

    @Test
    public void testPartitionKeyNotPushDown() {
        List<String> partitionKeys = Arrays.asList("b", "c", "d");

        Predicate predicateB =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 2, "b", Collections.singletonList(2));

        Predicate predicateC =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 3, "c", Collections.singletonList(3));

        Predicate predicateD =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 4, "d", Collections.singletonList(4));

        Predicate predicateDgreater =
                new LeafPredicate(
                        GreaterThan.INSTANCE,
                        DataTypes.INT(),
                        4,
                        "d",
                        Collections.singletonList(5));

        Predicate predicateE =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 5, "e", Collections.singletonList(5));

        /* filters only contain partition keys with func Equal ****************** */

        // where b=2 and c=3 and d=4, push down
        OnlyPartitionKeyEqualVisitor visitor = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(
                        PredicateBuilder.and(Arrays.asList(predicateB, predicateC, predicateD))
                                .visit(visitor))
                .isTrue();

        // where b=2 and c=3, push down
        OnlyPartitionKeyEqualVisitor visitor1 = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(PredicateBuilder.and(Arrays.asList(predicateB, predicateC)).visit(visitor1))
                .isTrue();

        // where b=2 and d=4, push down
        OnlyPartitionKeyEqualVisitor visitor2 = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(PredicateBuilder.and(Arrays.asList(predicateB, predicateD)).visit(visitor2))
                .isTrue();

        // where b=2 and c=3 and d=4 and e=5, do not push down
        OnlyPartitionKeyEqualVisitor visitor3 = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(
                        PredicateBuilder.and(
                                        Arrays.asList(
                                                predicateB, predicateC, predicateD, predicateE))
                                .visit(visitor3))
                .isFalse();

        // where b=2 and c=3 or d=4, do not push down
        OnlyPartitionKeyEqualVisitor visitor4 = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(
                        PredicateBuilder.and(
                                        Arrays.asList(
                                                PredicateBuilder.or(predicateB, predicateD),
                                                PredicateBuilder.or(predicateC, predicateD)))
                                .visit(visitor4))
                .isFalse();

        // where b=2 and c=3 and d>5, do not push down
        OnlyPartitionKeyEqualVisitor visitor5 = new OnlyPartitionKeyEqualVisitor(partitionKeys);
        assertThat(
                        PredicateBuilder.and(
                                        Arrays.asList(predicateB, predicateC, predicateDgreater))
                                .visit(visitor5))
                .isFalse();
    }
}
