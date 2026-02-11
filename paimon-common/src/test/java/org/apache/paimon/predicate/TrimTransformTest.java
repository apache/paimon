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

public class TrimTransformTest {

    @Test
    public void testNullInputs() {
        List<Object> inputs = new ArrayList<>();
        // test for single argument
        inputs.add(null);
        TrimTransform transform = new TrimTransform(inputs, "TRIM");
        Object result = transform.transform(GenericRow.of());
        assertThat(result).isNull();

        transform = new TrimTransform(inputs, "LTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isNull();

        transform = new TrimTransform(inputs, "RTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isNull();

        // test for binary argument
        inputs.add(null);
        transform = new TrimTransform(inputs, "TRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isNull();

        transform = new TrimTransform(inputs, "LTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isNull();

        transform = new TrimTransform(inputs, "RTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isNull();
    }

    @Test
    public void testNormalInputs() {
        // test trim('cd', 'cddcaadccd')
        List<Object> inputs = new ArrayList<>();
        inputs.add(BinaryString.fromString("cddcaadccd"));
        inputs.add(BinaryString.fromString("cd"));
        TrimTransform transform = new TrimTransform(inputs, "TRIM");
        Object result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("aa"));

        // test ltrim('cd', 'cddcaadccd')
        transform = new TrimTransform(inputs, "LTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("aadccd"));

        // test rtrim('cd', 'cddcaadccd')
        transform = new TrimTransform(inputs, "RTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("cddcaa"));

        // test trim(' aa  ')
        inputs.clear();
        inputs.add(BinaryString.fromString(" aa  "));
        transform = new TrimTransform(inputs, "TRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("aa"));

        // test trim(' aa  ')
        transform = new TrimTransform(inputs, "LTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString("aa  "));

        // test trim(' aa  ')
        transform = new TrimTransform(inputs, "RTRIM");
        result = transform.transform(GenericRow.of());
        assertThat(result).isEqualTo(BinaryString.fromString(" aa"));
    }

    @Test
    public void testSubstringRefInputs() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(new FieldRef(1, "f1", DataTypes.STRING()));
        inputs.add(new FieldRef(2, "f2", DataTypes.STRING()));
        TrimTransform transform = new TrimTransform(inputs, "TRIM");
        Object result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString("ahellob"),
                                BinaryString.fromString("ab")));
        assertThat(result).isEqualTo(BinaryString.fromString("hello"));

        transform = new TrimTransform(inputs, "LTRIM");
        result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString("ahellob"),
                                BinaryString.fromString("ab")));
        assertThat(result).isEqualTo(BinaryString.fromString("hellob"));

        transform = new TrimTransform(inputs, "RTRIM");
        result =
                transform.transform(
                        GenericRow.of(
                                BinaryString.fromString(""),
                                BinaryString.fromString("ahellob"),
                                BinaryString.fromString("ab")));
        assertThat(result).isEqualTo(BinaryString.fromString("ahello"));
    }
}
