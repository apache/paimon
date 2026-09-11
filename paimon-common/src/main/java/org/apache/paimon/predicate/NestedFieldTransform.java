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
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.InternalRowUtils.get;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Transform that extracts a field nested inside a row-typed column, for example {@code addr.city}.
 *
 * <p>The transform keeps the enclosing top-level column as its only {@link #inputs() input}, so
 * anything that rewrites field indices (schema projection, for instance) keeps working without
 * knowing about nesting. The positions below that column are held separately in {@link #path()}.
 *
 * <p>Deliberately <b>not</b> a {@link FieldTransform}: {@link LeafPredicate#fieldRefOptional()}
 * returns empty for it, which is what keeps every consumer that equates a leaf with a top-level
 * column — min/max pruning, file index lookup, ORC pushdown, schema evolution — from silently
 * reading the enclosing column's metadata as if it belonged to the nested field. Those consumers
 * give up on this transform instead, which costs pruning but never rows.
 */
public class NestedFieldTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "NESTED_FIELD_REF";

    public static final String FIELD_FIELD_REF = "fieldRef";
    public static final String FIELD_PATH = "path";

    /** The top-level row-typed column the nested field lives in. */
    private final FieldRef fieldRef;

    /**
     * Names of the fields to descend into, relative to {@code fieldRef}'s row type. Never empty.
     *
     * <p>Deliberately names rather than positions: {@link #copyWithNewInputs} may be handed a
     * structurally different row type — column masking and row filters remap that way — and a bare
     * position would stay in range while silently addressing whatever now sits there. Names are
     * re-resolved on every remap, so a reference either finds the same field or fails.
     */
    private final List<String> path;

    /** {@link #path} resolved to positions against {@code fieldRef}'s row type. */
    private final int[] positions;

    private final String name;
    private final DataType outputType;

    @JsonCreator
    public NestedFieldTransform(
            @JsonProperty(FIELD_FIELD_REF) FieldRef fieldRef,
            @JsonProperty(FIELD_PATH) List<String> path) {
        checkArgument(path != null && !path.isEmpty(), "Nested field path must not be empty.");
        this.fieldRef = fieldRef;
        this.path = Collections.unmodifiableList(new ArrayList<>(path));
        this.positions = new int[this.path.size()];

        StringBuilder nameBuilder = new StringBuilder(fieldRef.name());
        DataType current = fieldRef.type();
        for (int i = 0; i < this.path.size(); i++) {
            checkArgument(
                    current instanceof RowType,
                    "Nested field path of '%s' descends into a non-row type %s.",
                    fieldRef.name(),
                    current);
            RowType rowType = (RowType) current;
            String component = this.path.get(i);
            int position = rowType.getFieldIndex(component);
            checkArgument(
                    position >= 0,
                    "Nested field '%s' does not contain a field named '%s'.",
                    nameBuilder,
                    component);
            positions[i] = position;
            nameBuilder.append('.').append(component);
            current = rowType.getTypeAt(position);
        }
        this.name = nameBuilder.toString();
        this.outputType = current;
    }

    @Override
    public String name() {
        return NAME;
    }

    @JsonProperty(FIELD_FIELD_REF)
    public FieldRef fieldRef() {
        return fieldRef;
    }

    @JsonProperty(FIELD_PATH)
    public List<String> path() {
        return path;
    }

    /** Dot-separated name from the top-level column down to the nested field, {@code addr.city}. */
    @JsonIgnore
    public String fieldName() {
        return name;
    }

    @Override
    @JsonIgnore
    public List<Object> inputs() {
        return Collections.singletonList(fieldRef);
    }

    @Override
    @JsonIgnore
    public DataType outputType() {
        return outputType;
    }

    /**
     * Reads the nested field out of {@code row}, which must match the row type {@link #fieldRef}
     * was built against. A null anywhere along the path yields null, matching SQL semantics for
     * field access on a null struct.
     */
    @Override
    public Object transform(InternalRow row) {
        int position = fieldRef.index();
        if (row.isNullAt(position)) {
            return null;
        }
        RowType currentType = (RowType) fieldRef.type();
        InternalRow current = row.getRow(position, currentType.getFieldCount());

        for (int i = 0; i < positions.length - 1; i++) {
            position = positions[i];
            if (current.isNullAt(position)) {
                return null;
            }
            RowType nextType = (RowType) currentType.getTypeAt(position);
            current = current.getRow(position, nextType.getFieldCount());
            currentType = nextType;
        }

        int leaf = positions[positions.length - 1];
        return get(current, leaf, currentType.getTypeAt(leaf));
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        checkArgument(inputs.size() == 1);
        return new NestedFieldTransform((FieldRef) inputs.get(0), path);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NestedFieldTransform that = (NestedFieldTransform) o;
        return Objects.equals(fieldRef, that.fieldRef) && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldRef, path);
    }

    @Override
    public String toString() {
        return name;
    }
}
