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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartialUpdateMergeFunction}. */
public class PartialUpdateMergeFunctionTest {

    private long sequence = 0;

    @Test
    public void testUpdateNonNull() {
        Options options = new Options();
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        MergeFunction<KeyValue> func =
                PartialUpdateMergeFunction.factory(options, rowType).create();
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1, 1);
        add(func, 1, 2, 2, 2, 2, 2, null);
        validate(func, 1, 2, 2, 2, 2, 2, 1);
    }

    @Test
    public void testSequenceGroup() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2");
        options.set("fields.f6.sequence-group", "f4,f5");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        MergeFunction<KeyValue> func =
                PartialUpdateMergeFunction.factory(options, rowType).create();
        func.reset();
        add(func, 1, 1, 1, 1, 1, 1, 1);
        add(func, 1, 2, 2, 2, 2, 2, null);
        validate(func, 1, 2, 2, 2, 1, 1, 1);
        add(func, 1, 3, 3, 1, 3, 3, 3);
        validate(func, 1, 2, 2, 2, 3, 3, 3);
    }

    @Test
    public void testSequenceGroupRepeatDefine() {
        Options options = new Options();
        options.set("fields.f3.sequence-group", "f1,f2");
        options.set("fields.f4.sequence-group", "f1,f2");
        RowType rowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.INT());
        assertThatThrownBy(() -> PartialUpdateMergeFunction.factory(options, rowType))
                .hasMessageContaining("is defined repeatedly by multiple groups");
    }

    private void add(
            MergeFunction<KeyValue> function,
            Integer f0,
            Integer f1,
            Integer f2,
            Integer f3,
            Integer f4,
            Integer f5,
            Integer f6) {
        function.add(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                sequence++,
                                RowKind.INSERT,
                                GenericRow.of(f0, f1, f2, f3, f4, f5, f6)));
    }

    private void validate(
            MergeFunction<KeyValue> function,
            Integer f0,
            Integer f1,
            Integer f2,
            Integer f3,
            Integer f4,
            Integer f5,
            Integer f6) {
        assertThat(function.getResult().value())
                .isEqualTo(GenericRow.of(f0, f1, f2, f3, f4, f5, f6));
    }
}
