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

import java.util.List;
import java.util.Objects;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.types.DataTypeFamily.INTEGER_NUMERIC;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Substring {@link Transform}. */
public class SubstringTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "SUBSTRING";

    private final List<Object> inputs;

    public SubstringTransform(List<Object> inputs) {
        checkArgument(inputs.size() == 2 || inputs.size() == 3);
        this.inputs = inputs;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public final Object transform(InternalRow row) {
        Object source = inputs.get(0);
        BinaryString sourceString = null;
        if (source instanceof FieldRef) {
            FieldRef sourceFieldRef = (FieldRef) source;
            checkArgument(sourceFieldRef.type().is(CHARACTER_STRING));
            sourceString = row.isNullAt(0) ? null : row.getString(sourceFieldRef.index());
        } else {
            sourceString = (BinaryString) inputs.get(0);
        }
        if (sourceString == null) {
            return sourceString;
        }

        Object begin = inputs.get(1);
        int beginIndex;
        if (begin instanceof FieldRef) {
            FieldRef beginRef = (FieldRef) begin;
            checkArgument(beginRef.type().is(INTEGER_NUMERIC));
            beginIndex = row.getInt(beginRef.index());
        } else {
            beginIndex = Integer.parseInt(inputs.get(1).toString());
        }

        int endIndex = sourceString.getSizeInBytes();
        if (inputs.size() == 3) {
            Object end = inputs.get(2);
            if (end instanceof FieldRef) {
                FieldRef endRef = (FieldRef) inputs.get(2);
                checkArgument(endRef.type().is(INTEGER_NUMERIC));
                endIndex = row.getInt(endRef.index());
            } else {
                endIndex = Integer.parseInt(inputs.get(2).toString());
            }
        }
        return sourceString == null ? null : sourceString.substring(beginIndex, endIndex);
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new SubstringTransform(inputs);
    }

    @Override
    public final List<Object> inputs() {
        return inputs;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubstringTransform that = (SubstringTransform) o;
        return Objects.equals(inputs, that.inputs);
    }

    @Override
    public DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputs);
    }
}
