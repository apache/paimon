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

import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;

/** The {@link LeafFunction} to eval between. */
public class Between extends LeafFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "BETWEEN";

    public static final Between INSTANCE = new Between();

    @JsonCreator
    public Between() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null || literals.get(0) == null || literals.get(1) == null) {
            return false;
        }

        return compareLiteral(type, literals.get(0), field) <= 0
                && compareLiteral(type, literals.get(1), field) >= 0;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null) {
            if (rowCount == nullCount || literals.get(0) == null || literals.get(1) == null) {
                return false;
            }
        }

        // true if [min, max] and [l(0), l(1)] have intersection
        return compareLiteral(type, literals.get(0), max) <= 0
                && compareLiteral(type, literals.get(1), min) >= 0;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.empty();
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitBetween(fieldRef, literals.get(0), literals.get(1));
    }
}
