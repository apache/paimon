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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class PredicateJsonSerdeTest {
    @Test
    void testTransformPredicate() throws Exception {
        Transform transform =
                new ConcatTransform(
                        Arrays.asList(
                                new FieldRef(1, "col2", DataTypes.STRING()),
                                BinaryString.fromString("****")));
        TransformPredicate predicate =
                TransformPredicate.of(transform, IsNotNull.INSTANCE, Collections.emptyList());

        assertSerdeRoundTrip(predicate, TransformPredicate.class);
    }

    @Test
    void testLeafPredicateEqual() throws Exception {
        LeafPredicate predicate =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 0, "id", Collections.singletonList(100));

        assertSerdeRoundTrip(predicate, LeafPredicate.class);
    }

    @Test
    void testLeafPredicateLessThan() throws Exception {
        LeafPredicate predicate =
                new LeafPredicate(
                        LessThan.INSTANCE,
                        DataTypes.INT(),
                        1,
                        "age",
                        Collections.singletonList(50));

        assertSerdeRoundTrip(predicate, LeafPredicate.class);
    }

    @Test
    void testLeafPredicateIn() throws Exception {
        LeafPredicate predicate =
                new LeafPredicate(
                        In.INSTANCE,
                        DataTypes.STRING(),
                        2,
                        "status",
                        Arrays.asList(
                                BinaryString.fromString("active"),
                                BinaryString.fromString("pending")));

        assertSerdeRoundTrip(predicate, LeafPredicate.class);
    }

    @Test
    void testLeafPredicateIsNull() throws Exception {
        LeafPredicate predicate =
                new LeafPredicate(
                        IsNull.INSTANCE, DataTypes.STRING(), 3, "desc", Collections.emptyList());

        assertSerdeRoundTrip(predicate, LeafPredicate.class);
    }

    @Test
    void testCompoundPredicateAnd() throws Exception {
        LeafPredicate p1 =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 0, "id", Collections.singletonList(1));
        LeafPredicate p2 =
                new LeafPredicate(
                        LessThan.INSTANCE,
                        DataTypes.INT(),
                        1,
                        "age",
                        Collections.singletonList(30));
        CompoundPredicate predicate = new CompoundPredicate(And.INSTANCE, Arrays.asList(p1, p2));

        assertSerdeRoundTrip(predicate, CompoundPredicate.class);
    }

    @Test
    void testCompoundPredicateOr() throws Exception {
        LeafPredicate p1 =
                new LeafPredicate(
                        Equal.INSTANCE, DataTypes.INT(), 0, "id", Collections.singletonList(1));
        LeafPredicate p2 =
                new LeafPredicate(
                        LessThan.INSTANCE,
                        DataTypes.INT(),
                        1,
                        "age",
                        Collections.singletonList(30));
        CompoundPredicate predicate = new CompoundPredicate(Or.INSTANCE, Arrays.asList(p1, p2));

        assertSerdeRoundTrip(predicate, CompoundPredicate.class);
    }

    private void assertSerdeRoundTrip(Predicate predicate, Class<?> expectedType) throws Exception {
        String json = PredicateJsonSerde.toJsonString(predicate);
        Predicate parsed = PredicateJsonSerde.parse(json);
        assertThat(parsed).isInstanceOf(expectedType);
        assertThat(PredicateJsonSerde.toJsonString(parsed)).isEqualTo(json);
    }
}
