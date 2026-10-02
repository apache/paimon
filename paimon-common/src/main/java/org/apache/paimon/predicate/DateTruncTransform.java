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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** SQL {@code TRUNC(date, format)} transform. */
public class DateTruncTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "DATE_TRUNC";

    private final List<Object> inputs;

    @JsonCreator
    public DateTruncTransform(
            @JsonProperty(StringTransform.FIELD_INPUTS)
                    @JsonDeserialize(contentUsing = SubstringTransform.InputDeserializer.class)
                    List<Object> inputs) {
        checkArgument(inputs.size() == 2, "DATE_TRUNC requires exactly two inputs");
        TransformInputUtils.checkDate(inputs.get(0), "DATE_TRUNC first input must be a date");
        TransformInputUtils.checkString(inputs.get(1), "DATE_TRUNC format must be a string");
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
        return DataTypes.DATE();
    }

    @Override
    public Object transform(InternalRow row) {
        Integer epochDay = TransformInputUtils.integer(inputs.get(0), row);
        BinaryString format = TransformInputUtils.string(inputs.get(1), row);
        if (epochDay == null || format == null) {
            return null;
        }

        LocalDate date = LocalDate.ofEpochDay(epochDay);
        switch (format.toString().toUpperCase(Locale.ROOT)) {
            case "WEEK":
                date = date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
                break;
            case "MON":
            case "MONTH":
            case "MM":
                date = date.withDayOfMonth(1);
                break;
            case "QUARTER":
                date = date.withMonth(((date.getMonthValue() - 1) / 3) * 3 + 1).withDayOfMonth(1);
                break;
            case "YEAR":
            case "YYYY":
            case "YY":
                date = date.withDayOfYear(1);
                break;
            default:
                return null;
        }
        return Math.toIntExact(date.toEpochDay());
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return new DateTruncTransform(inputs);
    }

    @Override
    public boolean equals(Object o) {
        return o != null
                && getClass() == o.getClass()
                && Objects.equals(inputs, ((DateTruncTransform) o).inputs);
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
