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

class ConcatWsTransformTest {

    @Test
    public void testConcatWsLiteralInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString("-"));
        inputs.add(BinaryString.fromString("ha"));
        inputs.add(BinaryString.fromString("he"));
        ConcatWsTransform transform = new ConcatWsTransform(inputs);
        Object result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("ha-he"));
    }

    @Test
    public void testConcatWsRefInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(new FieldRef(2, "f2", DataTypes.STRING()));
        inputs.add(new FieldRef(1, "f1", DataTypes.STRING()));
        inputs.add(new FieldRef(3, "f3", DataTypes.STRING()));
        ConcatWsTransform transform = new ConcatWsTransform(inputs);
        Object result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString("ha"),
                                BinaryString.fromString("-"),
                                BinaryString.fromString("he")));
        assertThat(result).isEqualTo(BinaryString.fromString("ha-he"));
    }

    @Test
    public void testConcatWsHybridInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString("-"));
        inputs.add(BinaryString.fromString("ha"));
        inputs.add(new FieldRef(3, "f3", DataTypes.STRING()));
        ConcatWsTransform transform = new ConcatWsTransform(inputs);
        Object result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString(""),
                                BinaryString.fromString(""),
                                BinaryString.fromString("he")));
        assertThat(result).isEqualTo(BinaryString.fromString("ha-he"));
    }
}
