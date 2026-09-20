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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Objects;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Transform that returns the number of characters of a string, like SQL {@code CHAR_LENGTH}.
 *
 * <p>Unlike {@link StringTransform} its output is an {@code INT}, so it is a standalone {@link
 * Transform}. Input and JSON conventions follow {@link StringTransform}.
 */
public class LengthTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "LENGTH";

    private final List<Object> inputs;

    @JsonCreator
    public LengthTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = StringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        checkArgument(inputs.size() == 1, "LENGTH requires exactly one input");
        Object input = inputs.get(0);
        if (input instanceof FieldRef) {
            checkArgument(
                    ((FieldRef) input).type().is(CHARACTER_STRING),
                    "LENGTH input must be a string field");
        } else {
            checkArgument(
                    input == null || input instanceof BinaryString,
                    "LENGTH input literal must be a string");
        }
        this.inputs = inputs;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    @JsonIgnore
    public List<Object> inputs() {
        return inputs;
    }

    @JsonGetter(StringTransform.FIELD_INPUTS)
    public List<Object> inputsForJson() {
        return StringTransform.inputsForJson(inputs);
    }

    @Override
    public DataType outputType() {
        return DataTypes.INT();
    }

    @Override
    public Object transform(InternalRow row) {
        Object input = inputs.get(0);
        BinaryString value;
        if (input instanceof FieldRef) {
            FieldRef ref = (FieldRef) input;
            int i = ref.index();
            value = row.isNullAt(i) ? null : row.getString(i);
        } else {
            value = (BinaryString) input;
        }
        return value == null ? null : value.numChars();
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new LengthTransform(inputs);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LengthTransform that = (LengthTransform) o;
        return Objects.equals(inputs, that.inputs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputs);
    }

    @Override
    public String toString() {
        return StringTransform.formatCall(name(), inputs);
    }
}
