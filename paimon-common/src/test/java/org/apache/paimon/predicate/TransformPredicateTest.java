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
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TransformPredicateTest {

    @Test
    public void testReturnTrue() {
        TransformPredicate predicate = create();
        boolean result =
                predicate.test(
                        GenericRow.of(
                                BinaryString.fromString("ha"), BinaryString.fromString("-he")));
        assertThat(result).isTrue();
    }

    @Test
    public void testReturnFalse() {
        TransformPredicate predicate = create();
        boolean result =
                predicate.test(
                        GenericRow.of(
                                BinaryString.fromString("he"), BinaryString.fromString("-he")));
        assertThat(result).isFalse();
    }

    @Test
    public void testMinMax() {
        TransformPredicate predicate = create();
        boolean result = predicate.test(1, null, null, null);
        assertThat(result).isTrue();
    }

    @Test
    public void testClass() throws IOException, ClassNotFoundException {
        TransformPredicate predicate = create();
        TransformPredicate clone = InstantiationUtil.clone(predicate);
        assertThat(clone).isEqualTo(predicate);
        assertThat(clone.hashCode()).isEqualTo(predicate.hashCode());
        assertThat(clone.toString()).isEqualTo(predicate.toString());
    }

    private TransformPredicate create() {
        List<Object> inputs = new ArrayList<>();
        inputs.add(new FieldRef(0, "f0", DataTypes.STRING()));
        inputs.add(new FieldRef(1, "f1", DataTypes.STRING()));
        ConcatTransform transform = new ConcatTransform(inputs);
        List<Object> literals = new ArrayList<>();
        literals.add(BinaryString.fromString("ha-he"));
        return TransformPredicate.of(transform, Equal.INSTANCE, literals);
    }
}
