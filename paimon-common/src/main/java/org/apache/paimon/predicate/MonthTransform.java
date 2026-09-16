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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Optional;

/** Extracts the month of year, like SQL {@code EXTRACT(MONTH FROM d)}. */
public class MonthTransform extends DateExtractTransform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "MONTH";

    @JsonCreator
    public MonthTransform(@JsonProperty(DateExtractTransform.FIELD_FIELD_REF) FieldRef fieldRef) {
        super(fieldRef);
    }

    public static Optional<Transform> tryCreate(FieldRef fieldRef) {
        return DateExtractTransform.tryCreate(fieldRef, MonthTransform::new);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected Integer extract(LocalDateTime dateTime) {
        return dateTime.getMonthValue();
    }

    @Override
    protected Transform copy(FieldRef fieldRef) {
        return new MonthTransform(fieldRef);
    }
}
