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
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.NullTransform;
import org.apache.paimon.predicate.PartialMaskTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class PredicateJsonSerdeTest {

    @Test
    void testTransformToJsonAndParseTransform() throws Exception {
        Transform transform =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(0, "col1", DataTypes.STRING())));

        String json = TransformJsonSerde.toJsonString(transform);
        Transform parsed = TransformJsonSerde.parse(json);

        assertThat(parsed).isNotNull();
        assertThat(TransformJsonSerde.toJsonString(parsed)).isEqualTo(json);
    }

    @Test
    void testMaskTransformToJsonAndParseTransform() throws Exception {
        Transform transform =
                new PartialMaskTransform(
                        new FieldRef(0, "col1", DataTypes.STRING()),
                        2,
                        2,
                        BinaryString.fromString("*"));

        String json = TransformJsonSerde.toJsonString(transform);
        Transform parsed = TransformJsonSerde.parse(json);

        assertThat(parsed).isNotNull();
        assertThat(parsed).isInstanceOf(PartialMaskTransform.class);
        assertThat(TransformJsonSerde.toJsonString(parsed)).isEqualTo(json);
    }

    @Test
    void testNullTransformToJsonAndParseTransform() throws Exception {
        Transform transform = new NullTransform(new FieldRef(0, "col1", DataTypes.INT()));

        String json = TransformJsonSerde.toJsonString(transform);
        Transform parsed = TransformJsonSerde.parse(json);

        assertThat(parsed).isNotNull();
        assertThat(parsed).isInstanceOf(NullTransform.class);
        assertThat(TransformJsonSerde.toJsonString(parsed)).isEqualTo(json);
    }

    @Test
    void testPredicateToJsonAndParsePredicate() throws Exception {
        Transform transform =
                new ConcatTransform(
                        Arrays.asList(
                                new FieldRef(1, "col2", DataTypes.STRING()),
                                BinaryString.fromString("****")));

        TransformPredicate predicate =
                TransformPredicate.of(transform, IsNotNull.INSTANCE, Collections.emptyList());
        String json = PredicateJsonSerde.toJsonString(predicate);

        Predicate parsed = PredicateJsonSerde.parse(json);
        assertThat(parsed).isInstanceOf(TransformPredicate.class);
        assertThat(PredicateJsonSerde.toJsonString(parsed)).isEqualTo(json);
    }
}
