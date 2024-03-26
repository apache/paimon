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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/* test cases for DeleteFilter Push Down, suppose primaryKeys = (a,b,c,d), partitionKeys=(b,c,d), other fields = (e,f),
 * the following cases are tested:
 *
 * 1. where a=1 and b=2 and c=3 and d=4, push down
 * 2. where a=1 and b=2 and c=3 and d=4 and e is not null, push down
 * 3. where a=1 and b=2 and c=3 and d=4 and f=6, push down
 * 4. where a=1 and b=2 and c=3 and d=4 and e is not null and f=6, push down
 * 5. where a in (1,2) and b=2 and c=3 and d=4, push down
 * 6. where a=1 and b=1 and c is not null and d=4, do not push down
 * 7. where a=1, do not push down
 * 8. where a=1 and b=2 and d=4, do not push down
 * 9. where a=1 and c=3 and d=4, do not push down
 * 10. where b=2 and c=3 and d=4 and f=6, not push down
 *
 * 11. where b=2 and c=3 and d=4, push down
 * 12. where b=2 and c=3, push down
 * 13. where b=2 and d=4, push down
 * 14. where b=2 and c=3 and d=4 and e=5, do not push down
 * 15. where b=2 and c=3 or d=4, do not push down
 * 16. where b=2 and c=3 and d>5, do not push down
 */
/** DeletePushDownVisitorTest tests the DeletePushDownVisitor. */
public class DeletePushDownReaderTest {

    @Test
    public void testPrimaryKeyPushDown() {
        List<String> primaryKeys = Arrays.asList("a", "b", "c", "d");

        Predicate predicateA =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 1, "a", Collections.singletonList(1));

        Predicate predicateA1 =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 1, "a", Collections.singletonList(2));

        Predicate predicateB =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 2, "b", Collections.singletonList(2));

        Predicate predicateC =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 3, "c", Collections.singletonList(3));

        Predicate predicateCIsNotNull =
                new LeafPredicate(
                        IsNotNull.INSTANCE, DataTypes.INT(), 3, "c", Collections.singletonList(3));

        Predicate predicateD =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 4, "d", Collections.singletonList(4));

        // non-primary key's isNotNull filter
        Predicate predicateE =
                new LeafPredicate(
                        IsNotNull.INSTANCE, DataTypes.INT(), 5, "e", Collections.singletonList(5));

        Predicate predicateF =
                new LeafPredicate(
                        IsNotNull.INSTANCE, DataTypes.INT(), 6, "f", Collections.singletonList(6));

        /* filters contain all the primary keys with AND of Equal *********** */

        // where a=1 and b=2 and c=3 and d=4, push down
        AllPrimaryKeyEqualVisitor visitor = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate compoundPredicate =
                PredicateBuilder.and(Arrays.asList(predicateA, predicateB, predicateC, predicateD));
        Set<String> visitResult = compoundPredicate.visit(visitor);
        assertThat(visitResult).containsAnyElementsOf(primaryKeys);

        // where a=1 and b=2 and c=3 and d=4 and e is not null, push down
        AllPrimaryKeyEqualVisitor visitor1 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate predicateABCDE =
                PredicateBuilder.and(
                        Arrays.asList(predicateA, predicateB, predicateC, predicateD, predicateE));
        assertThat(predicateABCDE.visit(visitor1)).containsAnyElementsOf(primaryKeys);

        // where a=1 and b=2 and c=3 and d=4 and f=6, push down
        AllPrimaryKeyEqualVisitor visitor2 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate predicateABCDF =
                PredicateBuilder.and(
                        Arrays.asList(predicateA, predicateB, predicateC, predicateD, predicateF));
        assertThat(predicateABCDF.visit(visitor2)).containsAnyElementsOf(primaryKeys);

        // where a=1 and b=2 and c=3 and d=4 and e is not null and f=6, push down
        AllPrimaryKeyEqualVisitor visitor3 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate predicateABCDEF =
                PredicateBuilder.and(
                        Arrays.asList(
                                predicateA,
                                predicateB,
                                predicateC,
                                predicateD,
                                predicateE,
                                predicateF));
        assertThat(predicateABCDEF.visit(visitor3)).containsAnyElementsOf(primaryKeys);

        // where a in (1,2) and b=2 and c=3 and d=4, push down
        AllPrimaryKeyEqualVisitor visitor4 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate predicateAABCD =
                PredicateBuilder.and(
                        Arrays.asList(
                                PredicateBuilder.or(predicateA, predicateA1),
                                predicateB,
                                predicateC,
                                predicateD,
                                predicateE));
        assertThat(predicateAABCD.visit(visitor4)).containsAnyElementsOf(primaryKeys);

        /* not all the primary keys filters are of Equal func ************* */

        // where a=1 and b=1 and c is not null and d=4, do not push down
        AllPrimaryKeyEqualVisitor visitor5 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        Predicate predicateABCNotNull =
                PredicateBuilder.and(
                        Arrays.asList(predicateA, predicateB, predicateCIsNotNull, predicateD));
        assertThat(predicateABCNotNull.visit(visitor5)).isNotEqualTo(new HashSet<>(primaryKeys));

        /* filters not contain all the primary keys ****************** */

        // where a=1, do not push down
        AllPrimaryKeyEqualVisitor visitor6 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        assertThat(PredicateBuilder.and(Collections.singletonList(predicateA)).visit(visitor6))
                .isNotEqualTo(new HashSet<>(primaryKeys));

        // where a=1 and b=2 and d=4, do not push down
        AllPrimaryKeyEqualVisitor visitor7 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        assertThat(
                        PredicateBuilder.and(Arrays.asList(predicateA, predicateB, predicateD))
                                .visit(visitor7))
                .isNotEqualTo(new HashSet<>(primaryKeys));

        // where a=1 and c=3 and d=4, do not push down
        AllPrimaryKeyEqualVisitor visitor8 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        assertThat(
                        PredicateBuilder.and(Arrays.asList(predicateA, predicateC, predicateD))
                                .visit(visitor8))
                .isNotEqualTo(new HashSet<>(primaryKeys));

        // where b=2 and c=3 and d=4 and f=6, not push down
        AllPrimaryKeyEqualVisitor visitor9 = new AllPrimaryKeyEqualVisitor(primaryKeys);
        assertThat(
                        PredicateBuilder.and(
                                        Arrays.asList(
                                                predicateB, predicateC, predicateD, predicateF))
                                .visit(visitor9))
                .isNotEqualTo(new HashSet<>(primaryKeys));
    }

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
