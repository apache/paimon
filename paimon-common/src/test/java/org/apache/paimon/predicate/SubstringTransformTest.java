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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SubstringTransformTest {

    @Test
    public void testNullInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString(null));
        inputs.add(1);
        SubstringTransform transform = new SubstringTransform(inputs);
        Object result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(null);
    }

    @Test
    public void testNormalInputs() {
        // test substring('hello', 1)
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString("hello"));
        inputs.add(1);
        SubstringTransform transform = new SubstringTransform(inputs);
        Object result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("ello"));

        // test substring('hello', 1, 3)
        inputs.add(3);
        transform = new SubstringTransform(inputs);
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("el"));
    }

    @Test
    public void testSubstringRefInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(new FieldRef(1, "f1", DataTypes.STRING()));
        inputs.add(new FieldRef(3, "f3", DataTypes.INT()));
        inputs.add(new FieldRef(4, "f4", DataTypes.STRING()));
        SubstringTransform transform = new SubstringTransform(inputs);
        Object result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString("hello"),
                                BinaryString.fromString(""),
                                1,
                                3));
        assertThat(result).isEqualTo(BinaryString.fromString("el"));
    }
}
