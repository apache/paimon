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
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** SQL {@code LPAD} and {@code RPAD} transform. */
public class PadTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "PAD";
    public static final String FIELD_DIRECTION = "direction";

    private final List<Object> inputs;
    private final Direction direction;

    @JsonCreator
    public PadTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = SubstringTransform.InputDeserializer.class)
                    List<Object> inputs,
            @JsonProperty(FIELD_DIRECTION) Direction direction) {
        checkArgument(inputs.size() == 3, "PAD requires exactly three inputs");
        TransformInputUtils.checkString(inputs.get(0), "PAD input must be a string");
        TransformInputUtils.checkInteger(inputs.get(1), "PAD length must be an integer");
        TransformInputUtils.checkString(inputs.get(2), "PAD string must be a string");
        this.inputs = inputs;
        this.direction = checkNotNull(direction, "direction must not be null");
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

    @JsonGetter(FIELD_DIRECTION)
    public Direction direction() {
        return direction;
    }

    @Override
    public DataType outputType() {
        return DataTypes.STRING();
    }

    @Override
    public Object transform(InternalRow row) {
        BinaryString source = TransformInputUtils.string(inputs.get(0), row);
        Integer length = TransformInputUtils.integer(inputs.get(1), row);
        BinaryString pad = TransformInputUtils.string(inputs.get(2), row);
        if (source == null || length == null || pad == null) {
            return null;
        }

        int needed = length - source.numChars();
        if (needed <= 0 || pad.getSizeInBytes() == 0) {
            return source.substring(0, length);
        }

        int padChars = pad.numChars();
        StringBuilder padding = new StringBuilder();
        for (int i = 0; i < needed / padChars; i++) {
            padding.append(pad);
        }
        padding.append(pad.substring(0, needed % padChars));
        BinaryString padValue = BinaryString.fromString(padding.toString());
        return direction == Direction.LEFT
                ? BinaryString.concat(padValue, source)
                : BinaryString.concat(source, padValue);
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new PadTransform(inputs, direction);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PadTransform that = (PadTransform) o;
        return Objects.equals(inputs, that.inputs) && direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputs, direction);
    }

    @Override
    public String toString() {
        return (direction == Direction.LEFT ? "LPAD" : "RPAD")
                + StringTransform.formatCall("", inputs);
    }

    /** Pad side. */
    public enum Direction {
        LEFT,
        RIGHT
    }
}
