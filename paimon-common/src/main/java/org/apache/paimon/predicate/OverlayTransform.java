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

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** SQL {@code OVERLAY(input PLACING replacement FROM position [FOR length])} transform. */
public class OverlayTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "OVERLAY";

    private final List<Object> inputs;

    @JsonCreator
    public OverlayTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = SubstringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        checkArgument(
                inputs.size() == 3 || inputs.size() == 4, "OVERLAY requires three or four inputs");
        TransformInputUtils.checkString(inputs.get(0), "OVERLAY input must be a string");
        TransformInputUtils.checkString(inputs.get(1), "OVERLAY replacement must be a string");
        TransformInputUtils.checkInteger(inputs.get(2), "OVERLAY position must be an integer");
        if (inputs.size() == 4) {
            TransformInputUtils.checkInteger(inputs.get(3), "OVERLAY length must be an integer");
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
        return DataTypes.STRING();
    }

    @Override
    public Object transform(InternalRow row) {
        BinaryString source = TransformInputUtils.string(inputs.get(0), row);
        BinaryString replacement = TransformInputUtils.string(inputs.get(1), row);
        Integer position = TransformInputUtils.integer(inputs.get(2), row);
        Integer length =
                inputs.size() == 4 ? TransformInputUtils.integer(inputs.get(3), row) : null;
        if (source == null || replacement == null || position == null) {
            return null;
        }
        if (inputs.size() == 4 && length == null) {
            return null;
        }
        int replacedLength = length == null || length < 0 ? replacement.numChars() : length;
        return BinaryString.concat(
                source.substringSQL(1, position - 1),
                replacement,
                source.substringSQL(position + replacedLength, Integer.MAX_VALUE));
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new OverlayTransform(inputs);
    }

    @Override
    public boolean equals(Object o) {
        return o != null
                && getClass() == o.getClass()
                && Objects.equals(inputs, ((OverlayTransform) o).inputs);
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
