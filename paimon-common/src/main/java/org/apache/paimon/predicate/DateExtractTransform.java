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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.utils.InternalRowUtils.get;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Base {@link Transform} that extracts a calendar field from a {@code DATE} or {@code TIMESTAMP}
 * field, like SQL {@code EXTRACT(YEAR FROM d)} and the {@code year}, {@code month}, ... functions.
 * See the subclasses {@link YearTransform}, {@link MonthTransform}, {@link DayTransform}, {@link
 * HourTransform}, {@link MinuteTransform} and {@link SecondTransform}.
 *
 * <p>{@code TIMESTAMP WITH LOCAL TIME ZONE} is deliberately not supported: extracting a calendar
 * field from it depends on a session time zone, which the reader does not know, so such a predicate
 * must stay in the query engine.
 */
public abstract class DateExtractTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_FIELD_REF = "fieldRef";

    private final FieldRef fieldRef;

    protected DateExtractTransform(FieldRef fieldRef) {
        this.fieldRef = checkNotNull(fieldRef, "fieldRef must not be null");
        checkArgument(
                supported(fieldRef.type()),
                "%s requires a DATE or TIMESTAMP field, found %s",
                name(),
                fieldRef.type());
    }

    /** Creates a transform if {@code fieldRef} is a DATE or TIMESTAMP field. */
    protected static Optional<Transform> tryCreate(
            FieldRef fieldRef, Function<FieldRef, Transform> factory) {
        if (fieldRef == null || !supported(fieldRef.type())) {
            return Optional.empty();
        }
        return Optional.of(factory.apply(fieldRef));
    }

    private static boolean supported(DataType type) {
        switch (type.getTypeRoot()) {
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    @JsonGetter(FIELD_FIELD_REF)
    public FieldRef fieldRef() {
        return fieldRef;
    }

    @Override
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    @Override
    public DataType outputType() {
        return DataTypes.INT();
    }

    @Override
    public Object transform(InternalRow row) {
        Object value = get(row, fieldRef.index(), fieldRef.type());
        if (value == null) {
            return null;
        }
        LocalDateTime dateTime;
        if (value instanceof Timestamp) {
            dateTime = ((Timestamp) value).toLocalDateTime();
        } else {
            dateTime = LocalDate.ofEpochDay((Integer) value).atStartOfDay();
        }
        return extract(dateTime);
    }

    /** Extracts the calendar field this transform stands for. */
    protected abstract Integer extract(LocalDateTime dateTime);

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        checkArgument(inputs.size() == 1, "%s requires exactly one input", name());
        return copy((FieldRef) inputs.get(0));
    }

    /** Rebuilds this transform over a new field reference. */
    protected abstract Transform copy(FieldRef fieldRef);

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return Objects.equals(fieldRef, ((DateExtractTransform) o).fieldRef);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fieldRef);
    }

    @Override
    public String toString() {
        return name() + "(" + fieldRef + ")";
    }
}
