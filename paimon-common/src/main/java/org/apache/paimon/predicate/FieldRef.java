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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.ResolvedFieldPath;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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
    private final int[] nestedIndexes;
    private final int[] nestedArities;

    public FieldRef(int index, String name, DataType type) {
        this(index, name, type, null, null);
    }

    @JsonCreator
    public FieldRef(
            @JsonProperty(FIELD_INDEX) int index,
            @JsonProperty(FIELD_NAME) String name,
            @JsonProperty(FIELD_TYPE) DataType type,
            @JsonProperty(FIELD_NESTED_INDEXES) int[] nestedIndexes,
            @JsonProperty(FIELD_NESTED_ARITIES) int[] nestedArities) {
        int[] indexes = nestedIndexes == null ? new int[0] : nestedIndexes;
        int[] arities = nestedArities == null ? new int[0] : nestedArities;
        checkArgument(
                indexes.length == arities.length,
                "Nested indexes and arities must have the same length.");
        this.index = index;
        this.name = name;
        this.type = type;
        this.nestedIndexes = Arrays.copyOf(indexes, indexes.length);
        this.nestedArities = Arrays.copyOf(arities, arities.length);
    }

    /** Creates a field reference which preserves the structured positions of a resolved path. */
    public static FieldRef from(ResolvedFieldPath path) {
        return new FieldRef(
                path.topLevelIndex(),
                path.fullName(),
                path.leafField().type(),
                path.nestedIndexes(),
                path.nestedArities());
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
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int[] nestedIndexes() {
        return Arrays.copyOf(nestedIndexes, nestedIndexes.length);
    }

    @JsonProperty(FIELD_NESTED_ARITIES)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public int[] nestedArities() {
        return Arrays.copyOf(nestedArities, nestedArities.length);
    }

    public boolean isNested() {
        return nestedIndexes.length > 0;
    }

    /** Returns this reference with a remapped top-level field position. */
    public FieldRef withIndex(int newIndex) {
        return new FieldRef(newIndex, name, type, nestedIndexes, nestedArities);
    }

    /** Resolves the referenced field against a row type without losing structured path identity. */
    public Optional<DataField> resolveField(RowType rowType) {
        if (!isNested()) {
            return ResolvedFieldPath.resolve(rowType, name).map(ResolvedFieldPath::leafField);
        }

        if (index < 0 || index >= rowType.getFieldCount()) {
            return Optional.empty();
        }
        DataField field = rowType.getFields().get(index);
        for (int nestedIndex : nestedIndexes) {
            if (!(field.type() instanceof RowType)) {
                return Optional.empty();
            }
            RowType nestedRowType = (RowType) field.type();
            if (nestedIndex < 0 || nestedIndex >= nestedRowType.getFieldCount()) {
                return Optional.empty();
            }
            field = nestedRowType.getFields().get(nestedIndex);
        }
        return Optional.of(field);
    }

    /** Extracts this field from a row, returning null if any parent ROW is null. */
    public Object get(InternalRow row) {
        if (!isNested()) {
            return InternalRowUtils.get(row, index, type);
        }

        if (row.isNullAt(index)) {
            return null;
        }
        InternalRow nestedRow = row.getRow(index, nestedArities[0]);
        for (int i = 0; i < nestedIndexes.length; i++) {
            int nestedIndex = nestedIndexes[i];
            if (i == nestedIndexes.length - 1) {
                return InternalRowUtils.get(nestedRow, nestedIndex, type);
            }
            if (nestedRow.isNullAt(nestedIndex)) {
                return null;
            }
            nestedRow = nestedRow.getRow(nestedIndex, nestedArities[i + 1]);
        }
        throw new IllegalStateException("Invalid nested field reference: " + name);
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
