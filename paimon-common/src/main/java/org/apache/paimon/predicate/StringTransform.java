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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link Transform} with string inputs and string output. */
public abstract class StringTransform implements Transform {

    private static final long serialVersionUID = 1L;

    private final List<Object> inputs;

    public StringTransform(List<Object> inputs) {
        this.inputs = inputs;
        for (Object input : inputs) {
            if (input == null) {
                continue;
            }
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                checkArgument(ref.type().is(CHARACTER_STRING));
            } else {
                checkArgument(input instanceof BinaryString);
            }
        }
    }

    @Override
    public final List<Object> inputs() {
        return inputs;
    }

    @Override
    public final DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public final Object transform(InternalRow row) {
        List<BinaryString> strings = new ArrayList<>(inputs.size());
        for (Object input : inputs) {
            if (input instanceof FieldRef) {
                FieldRef ref = (FieldRef) input;
                int i = ref.index();
                strings.add(row.isNullAt(i) ? null : row.getString(i));
            } else {
                strings.add((BinaryString) input);
            }
        }
        return transform(strings);
    }

    protected abstract BinaryString transform(List<BinaryString> inputs);

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringTransform that = (StringTransform) o;
        return Objects.equals(inputs, that.inputs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputs);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + "inputs=" + inputs + '}';
    }
}
