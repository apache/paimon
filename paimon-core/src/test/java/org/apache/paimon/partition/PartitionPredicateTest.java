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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionPredicate}. */
public class PartitionPredicateTest {

    @Test
    public void test() {
        RowType type = DataTypes.ROW(DataTypes.INT(), DataTypes.INT());
        PredicateBuilder builder = new PredicateBuilder(type);
        Predicate predicate =
                or(
                        and(builder.equal(0, 3), builder.equal(1, 5)),
                        and(builder.equal(0, 4), builder.equal(1, 6)));

        PartitionPredicate p1 = PartitionPredicate.fromPredicate(type, predicate);
        PartitionPredicate p2 =
                PartitionPredicate.fromMultiple(
                        type, Arrays.asList(createPart(3, 5), createPart(4, 6)));

        assertThat(vailidate(p1, p2, createPart(3, 4))).isFalse();
        assertThat(vailidate(p1, p2, createPart(3, 5))).isTrue();
        assertThat(vailidate(p1, p2, createPart(4, 6))).isTrue();
        assertThat(vailidate(p1, p2, createPart(4, 5))).isFalse();

        assertThat(
                        vailidate(
                                p1,
                                new FieldStats[] {
                                    new FieldStats(4, 8, 0L), new FieldStats(10, 12, 0L)
                                }))
                .isFalse();
        assertThat(
                        vailidate(
                                p2,
                                new FieldStats[] {
                                    new FieldStats(4, 8, 0L), new FieldStats(10, 12, 0L)
                                }))
                .isTrue();
        assertThat(
                        vailidate(
                                p2,
                                new FieldStats[] {
                                    new FieldStats(6, 8, 0L), new FieldStats(10, 12, 0L)
                                }))
                .isFalse();

        assertThat(
                        vailidate(
                                p1,
                                new FieldStats[] {
                                    new FieldStats(4, 8, 0L), new FieldStats(5, 12, 0L)
                                }))
                .isTrue();
        assertThat(
                        vailidate(
                                p2,
                                new FieldStats[] {
                                    new FieldStats(4, 8, 0L), new FieldStats(5, 12, 0L)
                                }))
                .isTrue();

        assertThat(
                        vailidate(
                                p1,
                                new FieldStats[] {
                                    new FieldStats(1, 2, 0L), new FieldStats(2, 3, 0L)
                                }))
                .isFalse();
        assertThat(
                        vailidate(
                                p2,
                                new FieldStats[] {
                                    new FieldStats(1, 2, 0L), new FieldStats(2, 3, 0L)
                                }))
                .isFalse();
    }

    private boolean vailidate(
            PartitionPredicate predicate1, PartitionPredicate predicate2, BinaryRow part) {
        boolean ret = predicate1.test(part);
        assertThat(predicate2.test(part)).isEqualTo(ret);
        return ret;
    }

    private boolean vailidate(PartitionPredicate predicate, FieldStats[] fieldStats) {
        return predicate.test(3, fieldStats);
    }

    private static BinaryRow createPart(int i, int j) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, i);
        writer.writeInt(1, j);
        writer.complete();
        return row;
    }
}
