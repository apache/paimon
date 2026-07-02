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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** A reference to a field in an input. */
public class FieldRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_NESTED_INDEXES = "nestedIndexes";
    private static final String FIELD_NESTED_ARITIES = "nestedArities";

    private final int index;
    private final String name;
    private final DataType type;
    @Nullable private final int[] nestedIndexes;
    @Nullable private final int[] nestedArities;

    public FieldRef(int index, String name, DataType type) {
        this(index, name, type, null, null);
    }

    @JsonCreator
    public FieldRef(
            @JsonProperty(FIELD_INDEX) int index,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_TYPE) DataType type,
            @JsonProperty(FIELD_NESTED_INDEXES) @Nullable int[] nestedIndexes,
            @JsonProperty(FIELD_NESTED_ARITIES) @Nullable int[] nestedArities) {
        this.index = index;
        this.name = name;
        this.type = type;
        this.nestedIndexes = nestedIndexes;
        this.nestedArities = nestedArities;
    }

    @JsonProperty(FIELD_INDEX)
    public int index() {
        return index;
    }

    @JsonProperty(FIELD_NAME)
    public String name() {
        return name;
    }

    @JsonProperty(FIELD_TYPE)
    public DataType type() {
        return type;
    }

    @JsonProperty(FIELD_NESTED_INDEXES)
    @Nullable
    public int[] nestedIndexes() {
        return nestedIndexes;
    }

    @JsonProperty(FIELD_NESTED_ARITIES)
    @Nullable
    public int[] nestedArities() {
        return nestedArities;
    }

    /**
     * Returns a copy of this {@link FieldRef} with the given top-level index, preserving name,
     * type, and any nested path metadata. Use this instead of the constructor when remapping a
     * {@link FieldRef} to a new schema (e.g. projection, partition, or auth remapping), so that
     * nested field predicates keep working after the rewrite.
     */
    public FieldRef withIndex(int newIndex) {
        return new FieldRef(newIndex, name, type, nestedIndexes, nestedArities);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldRef fieldRef = (FieldRef) o;
        return index == fieldRef.index
                && Objects.equals(name, fieldRef.name)
                && Objects.equals(type, fieldRef.type)
                && Arrays.equals(nestedIndexes, fieldRef.nestedIndexes)
                && Arrays.equals(nestedArities, fieldRef.nestedArities);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(index, name, type);
        result = 31 * result + Arrays.hashCode(nestedIndexes);
        result = 31 * result + Arrays.hashCode(nestedArities);
        return result;
    }

    @Override
    public String toString() {
        return name;
    }
}
