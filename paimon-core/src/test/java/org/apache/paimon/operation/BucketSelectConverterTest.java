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

package org.apache.paimon.operation;

import org.apache.paimon.operation.BucketSelectConverter.Selector;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.or;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketSelectConverter}. */
public class BucketSelectConverterTest {

    private final RowType rowType = RowType.of(new IntType(), new IntType(), new IntType());

    private final PredicateBuilder builder = new PredicateBuilder(rowType);

    @Test
    public void testRepeatEqual() {
        assertThat(newSelector(and(builder.equal(0, 0), builder.equal(0, 1)))).isEmpty();
    }

    @Test
    public void testNotFull() {
        assertThat(newSelector(and(builder.equal(0, 0)))).isEmpty();
    }

    @Test
    public void testOtherPredicate() {
        assertThat(newSelector(and(builder.notEqual(0, 0)))).isEmpty();
    }

    @Test
    public void testOrIllegal() {
        assertThat(
                        newSelector(
                                and(
                                        or(builder.equal(0, 5), builder.equal(1, 6)),
                                        builder.equal(1, 1),
                                        builder.equal(2, 2))))
                .isEmpty();
    }

    @Test
    public void testNormal() {
        Selector selector =
                newSelector(and(builder.equal(0, 0), builder.equal(1, 1), builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes()).containsExactly(1141287431);
        assertThat(selector.createBucketSet(20)).containsExactly(11);
    }

    @Test
    public void testIn() {
        Selector selector =
                newSelector(
                                and(
                                        builder.in(0, Arrays.asList(5, 6, 7)),
                                        builder.equal(1, 1),
                                        builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes()).containsExactly(-1272936927, 582056914, -1234868890);
        assertThat(selector.createBucketSet(20)).containsExactly(7, 14, 10);
    }

    @Test
    public void testOr() {
        Selector selector =
                newSelector(
                                and(
                                        or(
                                                builder.equal(0, 5),
                                                builder.equal(0, 6),
                                                builder.equal(0, 7)),
                                        builder.equal(1, 1),
                                        builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes()).containsExactly(-1272936927, 582056914, -1234868890);
        assertThat(selector.createBucketSet(20)).containsExactly(7, 14, 10);
    }

    @Test
    public void testInNull() {
        Selector selector =
                newSelector(
                                and(
                                        builder.in(0, Arrays.asList(5, 6, 7, null)),
                                        builder.equal(1, 1),
                                        builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes()).containsExactly(-1272936927, 582056914, -1234868890);
        assertThat(selector.createBucketSet(20)).containsExactly(7, 14, 10);
    }

    @Test
    public void testMultipleIn() {
        Selector selector =
                newSelector(
                                and(
                                        builder.in(0, Arrays.asList(5, 6, 7)),
                                        builder.in(1, Arrays.asList(1, 8)),
                                        builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes())
                .containsExactly(
                        -1272936927, -1567268077, 582056914, 2124429589, -1234868890, 1063796399);
        assertThat(selector.createBucketSet(20)).containsExactly(7, 17, 14, 9, 10, 19);
    }

    @Test
    public void testMultipleOr() {
        Selector selector =
                newSelector(
                                and(
                                        or(
                                                builder.equal(0, 5),
                                                builder.equal(0, 6),
                                                builder.equal(0, 7)),
                                        or(builder.equal(1, 1), builder.equal(1, 8)),
                                        builder.equal(2, 2)))
                        .get();
        assertThat(selector.hashCodes())
                .containsExactly(
                        -1272936927, -1567268077, 582056914, 2124429589, -1234868890, 1063796399);
        assertThat(selector.createBucketSet(20)).containsExactly(7, 17, 14, 9, 10, 19);
    }

    private Optional<Selector> newSelector(Predicate predicate) {
        return (Optional) BucketSelectConverter.create(predicate, rowType);
    }
}
