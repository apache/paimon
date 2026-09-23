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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
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

    @JsonCreator
    public SubstringTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = InputDeserializer.class)
                    List<Object> inputs) {
        checkArgument(inputs.size() == 2 || inputs.size() == 3);
        Object source = inputs.get(0);
        // transform() casts this slot to BinaryString
        checkArgument(
                source == null || source instanceof FieldRef || source instanceof BinaryString,
                "SUBSTRING source must be a string or a field reference");
        this.inputs = inputs;
    }

    /** Deserializer for {@link SubstringTransform} inputs, which may also be integers. */
    public static class InputDeserializer extends StringTransform.InputDeserializer {

        private static final long serialVersionUID = 1L;

        @Override
        protected Object otherInput(JsonNode node, DeserializationContext context)
                throws IOException {
            if (node.isNumber()) {
                // canConvertToInt checks the range, not integrality
                if (!node.isIntegralNumber()) {
                    context.reportInputMismatch(
                            Object.class,
                            "SubstringTransform position must be an integer: %s",
                            node.toString());
                }
                return node.canConvertToInt() ? node.intValue() : node.numberValue();
            }
            return super.otherInput(node, context);
        }
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
            int sourceIndex = sourceFieldRef.index();
            sourceString = row.isNullAt(sourceIndex) ? null : row.getString(sourceIndex);
        } else {
            sourceString = (BinaryString) inputs.get(0);
        }
        if (sourceString == null) {
            return sourceString;
        }

        // SQL null propagation: any null input yields null, whether it arrives as a
        // literal or as a null value in a referenced field
        if (isNullPosition(inputs.get(1), row)) {
            return null;
        }
        boolean hasLength = inputs.size() == 3;
        if (hasLength && isNullPosition(inputs.get(2), row)) {
            return null;
        }

        int pos = readPosition(inputs.get(1), row);
        int length = hasLength ? readPosition(inputs.get(2), row) : Integer.MAX_VALUE;
        return sourceString.substringSQL(pos, length);
    }

    private static boolean isNullPosition(Object position, InternalRow row) {
        if (position == null) {
            return true;
        }
        if (position instanceof FieldRef) {
            FieldRef ref = (FieldRef) position;
            checkArgument(ref.type().is(INTEGER_NUMERIC));
            // getInt on a null throws on GenericRow and reads an undefined value on columnar rows
            return row.isNullAt(ref.index());
        }
        return false;
    }

    private static int readPosition(Object position, InternalRow row) {
        if (position instanceof FieldRef) {
            FieldRef ref = (FieldRef) position;
            switch (ref.type().getTypeRoot()) {
                case TINYINT:
                    return row.getByte(ref.index());
                case SMALLINT:
                    return row.getShort(ref.index());
                case INTEGER:
                    return row.getInt(ref.index());
                case BIGINT:
                    return Math.toIntExact(row.getLong(ref.index()));
                default:
                    throw new IllegalArgumentException(
                            "Unsupported substring position type: " + ref.type());
            }
        }
        return Integer.parseInt(position.toString());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new SubstringTransform(inputs);
    }

    @Override
    @JsonIgnore
    public final List<Object> inputs() {
        return inputs;
    }

    @JsonGetter(StringTransform.FIELD_INPUTS)
    public final List<Object> inputsForJson() {
        return StringTransform.inputsForJson(inputs);
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

    @Override
    public String toString() {
        return StringTransform.formatCall(name(), inputs);
    }
}
