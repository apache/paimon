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

/** SQL {@code DATE_DIFF(end_date, start_date)} transform. */
public class DateDiffTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "DATE_DIFF";

    private final List<Object> inputs;

    @JsonCreator
    public DateDiffTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = SubstringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        checkArgument(inputs.size() == 2, "DATE_DIFF requires exactly two inputs");
        TransformInputUtils.checkDate(inputs.get(0), "DATE_DIFF first input must be a date");
        TransformInputUtils.checkDate(inputs.get(1), "DATE_DIFF second input must be a date");
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
        Integer end = TransformInputUtils.integer(inputs.get(0), row);
        Integer start = TransformInputUtils.integer(inputs.get(1), row);
        return end == null || start == null ? null : end - start;
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new DateDiffTransform(inputs);
    }

    @Override
    public boolean equals(Object o) {
        return o != null
                && getClass() == o.getClass()
                && Objects.equals(inputs, ((DateDiffTransform) o).inputs);
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
