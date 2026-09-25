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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LengthTransformTest {

    @Test
    public void testLiteralInput() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString("hello"));
        LengthTransform transform = new LengthTransform(inputs);
        assertThat(transform.transform(GenericRow.of())).isEqualTo(5);
        assertThat(transform.outputType()).isEqualTo(DataTypes.INT());
    }

    @Test
    public void testFieldInput() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(new FieldRef(0, "f0", DataTypes.STRING()));
        LengthTransform transform = new LengthTransform(inputs);
        assertThat(transform.transform(GenericRow.of(BinaryString.fromString("paimon"))))
                .isEqualTo(6);
        assertThat(transform.transform(GenericRow.of((Object) null))).isNull();
    }

    @Test
    public void testNullInputSlot() {
        LengthTransform transform = new LengthTransform(Collections.singletonList(null));
        assertThat(transform.transform(GenericRow.of())).isNull();
    }

    @Test
    public void testIllegalInputs() {
        // wrong arity
        List<Object> two = new ArrayList<>();
        two.add(BinaryString.fromString("hello"));
        two.add(BinaryString.fromString("hi"));
        assertThatThrownBy(() -> new LengthTransform(two))
                .isInstanceOf(IllegalArgumentException.class);

        // non-string field
        assertThatThrownBy(
                        () ->
                                new LengthTransform(
                                        Collections.singletonList(
                                                new FieldRef(0, "f0", DataTypes.INT()))))
                .isInstanceOf(IllegalArgumentException.class);

        // non-string literal
        assertThatThrownBy(() -> new LengthTransform(Collections.singletonList((Object) 5)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
