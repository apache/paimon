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

package org.apache.flink.table.store.data.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;

import java.io.Serializable;
import java.time.Period;

/** Converter for {@link YearMonthIntervalType} of {@link Period} external type. */
@Internal
public class YearMonthIntervalPeriodConverter implements DataStructureConverter<Integer, Period> {

    private static final long serialVersionUID = 1L;

    private final PeriodConstructor periodConstructor;

    private YearMonthIntervalPeriodConverter(PeriodConstructor periodConstructor) {
        this.periodConstructor = periodConstructor;
    }

    @Override
    public Integer toInternal(Period external) {
        return (int) external.toTotalMonths();
    }

    @Override
    public Period toExternal(Integer internal) {
        return periodConstructor.construct(internal);
    }

    private interface PeriodConstructor extends Serializable {
        Period construct(Integer internal);
    }

    // --------------------------------------------------------------------------------------------
    // Factory method
    // --------------------------------------------------------------------------------------------

    public static YearMonthIntervalPeriodConverter create(DataType dataType) {
        return create((YearMonthIntervalType) dataType.getLogicalType());
    }

    public static YearMonthIntervalPeriodConverter create(YearMonthIntervalType intervalType) {
        return new YearMonthIntervalPeriodConverter(
                createPeriodConstructor(intervalType.getResolution()));
    }

    private static PeriodConstructor createPeriodConstructor(YearMonthResolution resolution) {
        switch (resolution) {
            case YEAR:
                return internal -> Period.ofYears(internal / 12);
            case YEAR_TO_MONTH:
                return internal -> Period.of(internal / 12, internal % 12, 0);
            case MONTH:
                return Period::ofMonths;
            default:
                throw new IllegalStateException();
        }
    }
}
