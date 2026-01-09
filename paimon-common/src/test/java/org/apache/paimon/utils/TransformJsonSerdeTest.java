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
import org.apache.paimon.predicate.CastTransform;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TransformJsonSerde}. */
class TransformJsonSerdeTest {

    @Test
    void testFieldTransform() throws Exception {
        // Test various data types
        DataType[] dataTypes = {
            DataTypes.STRING(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.DOUBLE(),
            DataTypes.BOOLEAN(),
            DataTypes.DATE(),
            DataTypes.TIMESTAMP()
        };

        for (int i = 0; i < dataTypes.length; i++) {
            FieldRef fieldRef = new FieldRef(i, "col_" + i, dataTypes[i]);
            Transform transform = new FieldTransform(fieldRef);

            assertSerdeRoundTrip(transform);

            Transform parsed = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform));
            assertThat(((FieldTransform) parsed).fieldRef()).isEqualTo(fieldRef);
        }
    }

    @Test
    void testUpperTransform() throws Exception {
        FieldRef fieldRef = new FieldRef(1, "col1", DataTypes.STRING());
        Transform transform = new UpperTransform(Collections.singletonList(fieldRef));

        assertSerdeRoundTrip(transform);

        Transform parsed = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform));
        assertThat(parsed.inputs()).hasSize(1);
        assertThat(parsed.inputs().get(0)).isEqualTo(fieldRef);
    }

    @Test
    void testConcatTransform() throws Exception {
        // Test with multiple fields
        FieldRef fieldRef1 = new FieldRef(0, "first_name", DataTypes.STRING());
        FieldRef fieldRef2 = new FieldRef(1, "last_name", DataTypes.STRING());
        Transform transform1 = new ConcatTransform(Arrays.asList(fieldRef1, fieldRef2));

        assertSerdeRoundTrip(transform1);

        Transform parsed1 = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform1));
        assertThat(parsed1.inputs()).hasSize(2);
        assertThat(parsed1.inputs().get(0)).isEqualTo(fieldRef1);
        assertThat(parsed1.inputs().get(1)).isEqualTo(fieldRef2);

        // Test with field and literal
        FieldRef fieldRef = new FieldRef(0, "name", DataTypes.STRING());
        BinaryString literal = BinaryString.fromString("_suffix");
        Transform transform2 = new ConcatTransform(Arrays.asList(fieldRef, literal));

        assertSerdeRoundTrip(transform2);

        Transform parsed2 = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform2));
        assertThat(parsed2.inputs()).hasSize(2);
        assertThat(parsed2.inputs().get(0)).isEqualTo(fieldRef);
        assertThat(parsed2.inputs().get(1)).isEqualTo(literal);
    }

    @Test
    void testConcatWsTransform() throws Exception {
        // Test with multiple fields
        BinaryString separator1 = BinaryString.fromString(" ");
        FieldRef fieldRef1 = new FieldRef(0, "first_name", DataTypes.STRING());
        FieldRef fieldRef2 = new FieldRef(1, "last_name", DataTypes.STRING());
        Transform transform1 =
                new ConcatWsTransform(Arrays.asList(separator1, fieldRef1, fieldRef2));

        assertSerdeRoundTrip(transform1);

        Transform parsed1 = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform1));
        assertThat(parsed1.inputs()).hasSize(3);
        assertThat(parsed1.inputs().get(0)).isEqualTo(separator1);
        assertThat(parsed1.inputs().get(1)).isEqualTo(fieldRef1);
        assertThat(parsed1.inputs().get(2)).isEqualTo(fieldRef2);

        // Test with null value
        BinaryString separator2 = BinaryString.fromString(",");
        FieldRef fieldRef = new FieldRef(0, "col1", DataTypes.STRING());
        Transform transform2 = new ConcatWsTransform(Arrays.asList(separator2, fieldRef, null));

        assertSerdeRoundTrip(transform2);

        Transform parsed2 = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform2));
        assertThat(parsed2.inputs()).hasSize(3);
        assertThat(parsed2.inputs().get(0)).isEqualTo(separator2);
        assertThat(parsed2.inputs().get(1)).isEqualTo(fieldRef);
        assertThat(parsed2.inputs().get(2)).isNull();
    }

    @Test
    void testCastTransform() throws Exception {
        // Test various cast scenarios
        Object[][] testCases = {
            {new FieldRef(0, "age", DataTypes.STRING()), DataTypes.INT()},
            {new FieldRef(1, "id", DataTypes.BIGINT()), DataTypes.INT()},
            {new FieldRef(2, "count", DataTypes.INT()), DataTypes.STRING()}
        };

        for (Object[] testCase : testCases) {
            FieldRef fieldRef = (FieldRef) testCase[0];
            DataType targetType = (DataType) testCase[1];

            Transform transform =
                    CastTransform.tryCreate(fieldRef, targetType)
                            .orElseThrow(() -> new RuntimeException("Cast not supported"));

            assertSerdeRoundTrip(transform);

            Transform parsed = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform));
            assertThat(parsed.inputs()).hasSize(1);
            assertThat(parsed.inputs().get(0)).isEqualTo(fieldRef);
            assertThat(parsed.outputType()).isEqualTo(targetType);
        }
    }

    @Test
    void testComplexTransform() throws Exception {
        // Test a complex transform with multiple fields, literals and null
        FieldRef fieldRef1 = new FieldRef(0, "prefix", DataTypes.STRING());
        BinaryString separator = BinaryString.fromString("-");
        FieldRef fieldRef2 = new FieldRef(1, "id", DataTypes.STRING());
        BinaryString suffix = BinaryString.fromString("_end");

        Transform transform =
                new ConcatWsTransform(Arrays.asList(separator, fieldRef1, fieldRef2, suffix, null));

        assertSerdeRoundTrip(transform);

        Transform parsed = TransformJsonSerde.parse(TransformJsonSerde.toJsonString(transform));
        assertThat(parsed.inputs()).hasSize(5);
        assertThat(parsed.inputs().get(0)).isEqualTo(separator);
        assertThat(parsed.inputs().get(1)).isEqualTo(fieldRef1);
        assertThat(parsed.inputs().get(2)).isEqualTo(fieldRef2);
        assertThat(parsed.inputs().get(3)).isEqualTo(suffix);
        assertThat(parsed.inputs().get(4)).isNull();
    }

    private void assertSerdeRoundTrip(Transform transform) throws Exception {
        String json = TransformJsonSerde.toJsonString(transform);
        assertThat(json).isNotNull();

        Transform parsed = TransformJsonSerde.parse(json);
        assertThat(parsed).isInstanceOf(transform.getClass());

        String jsonAfterParse = TransformJsonSerde.toJsonString(parsed);
        assertThat(jsonAfterParse).isEqualTo(json);
    }
}
