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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/** Evaluate the predicate result based on multiple {@link Predicate}s. */
public abstract class CompoundFunction implements Serializable {

    @JsonCreator
    public static CompoundFunction fromJson(String name) throws IOException {
        switch (name) {
            case And.NAME:
                return And.INSTANCE;
            case Or.NAME:
                return Or.INSTANCE;
            default:
                throw new IllegalArgumentException(
                        "Could not resolve compound predicate function '" + name + "'");
        }
    }

    @JsonValue
    public String toJson() {
        if (this instanceof And) {
            return And.NAME;
        } else if (this instanceof Or) {
            return Or.NAME;
        } else {
            throw new IllegalArgumentException(
                    "Unknown compound predicate function class for JSON serialization: "
                            + getClass());
        }
    }

    public abstract boolean test(InternalRow row, List<Predicate> children);

    public abstract boolean test(
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts,
            List<Predicate> children);

    public abstract Optional<Predicate> negate(List<Predicate> children);

    public abstract <T> T visit(FunctionVisitor<T> visitor, List<T> children);

    @Override
    public int hashCode() {
        return this.getClass().getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
