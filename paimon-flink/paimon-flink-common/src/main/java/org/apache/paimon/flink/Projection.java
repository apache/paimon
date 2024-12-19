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

package org.apache.paimon.flink;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypeVisitor;

import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/**
 * {@link Projection} represents a list of (possibly nested) indexes that can be used to project
 * data types. A row projection includes both reducing the accessible fields and reordering them.
 */
public abstract class Projection {

    // sealed class
    private Projection() {}

    public abstract RowType project(RowType logicalType);

    public abstract org.apache.paimon.types.RowType project(
            org.apache.paimon.types.RowType rowType);

    public abstract ProjectionRowData getOuterProjectRow(org.apache.paimon.types.RowType rowType);

    /** @return {@code true} whether this projection is nested or not. */
    public abstract boolean isNested();

    /**
     * Convert this instance to a projection of top level indexes. The array represents the mapping
     * of the fields of the original {@link DataType}. For example, {@code [0, 2, 1]} specifies to
     * include in the following order the 1st field, the 3rd field and the 2nd field of the row.
     *
     * @throws IllegalStateException if this projection is nested.
     */
    public abstract int[] toTopLevelIndexes();

    /**
     * Convert this instance to a nested projection index paths. The array represents the mapping of
     * the fields of the original {@link DataType}, including nested rows. For example, {@code [[0,
     * 2, 1], ...]} specifies to include the 2nd field of the 3rd field of the 1st field in the
     * top-level row.
     */
    public abstract int[][] toNestedIndexes();

    /**
     * Create an empty {@link Projection}, that is a projection that projects no fields, returning
     * an empty {@link DataType}.
     */
    public static Projection empty() {
        return EmptyProjection.INSTANCE;
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toTopLevelIndexes()
     */
    public static Projection of(int[] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new TopLevelProjection(indexes);
    }

    /**
     * Create a {@link Projection} of the provided {@code indexes}.
     *
     * @see #toNestedIndexes()
     */
    public static Projection of(int[][] indexes) {
        if (indexes.length == 0) {
            return empty();
        }
        return new NestedProjection(indexes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Projection)) {
            return false;
        }
        Projection other = (Projection) o;
        if (!this.isNested() && !other.isNested()) {
            return Arrays.equals(this.toTopLevelIndexes(), other.toTopLevelIndexes());
        }
        return Arrays.deepEquals(this.toNestedIndexes(), other.toNestedIndexes());
    }

    @Override
    public int hashCode() {
        if (isNested()) {
            return Arrays.deepHashCode(toNestedIndexes());
        }
        return Arrays.hashCode(toTopLevelIndexes());
    }

    @Override
    public String toString() {
        if (isNested()) {
            return "Nested projection = " + Arrays.deepToString(toNestedIndexes());
        }
        return "Top level projection = " + Arrays.toString(toTopLevelIndexes());
    }

    private static class EmptyProjection extends Projection {

        static final EmptyProjection INSTANCE = new EmptyProjection();

        private EmptyProjection() {}

        @Override
        public RowType project(RowType dataType) {
            return new NestedProjection(toNestedIndexes()).project(dataType);
        }

        @Override
        public org.apache.paimon.types.RowType project(org.apache.paimon.types.RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public ProjectionRowData getOuterProjectRow(org.apache.paimon.types.RowType rowType) {
            return new NestedProjection(toNestedIndexes()).getOuterProjectRow(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public int[] toTopLevelIndexes() {
            return new int[0];
        }

        @Override
        public int[][] toNestedIndexes() {
            return new int[0][];
        }
    }

    private static class NestedProjection extends Projection {

        final int[][] projection;
        final boolean nested;

        NestedProjection(int[][] projection) {
            this.projection = projection;
            this.nested = Arrays.stream(projection).anyMatch(arr -> arr.length > 1);
        }

        @Override
        public RowType project(RowType rowType) {
            final List<RowType.RowField> updatedFields = new ArrayList<>();
            Set<String> nameDomain = new HashSet<>();
            int duplicateCount = 0;
            for (int[] indexPath : this.projection) {
                RowType.RowField field = rowType.getFields().get(indexPath[0]);
                StringBuilder builder =
                        new StringBuilder(rowType.getFieldNames().get(indexPath[0]));
                for (int index = 1; index < indexPath.length; index++) {
                    RowType rowtype = ((RowType) field.getType());
                    builder.append("_").append(rowtype.getFieldNames().get(indexPath[index]));
                    field = rowtype.getFields().get(indexPath[index]);
                }
                String path = builder.toString();
                while (nameDomain.contains(path)) {
                    path = builder.append("_$").append(duplicateCount++).toString();
                }
                updatedFields.add(
                        new RowType.RowField(
                                path, field.getType(), field.getDescription().orElse(null)));
                nameDomain.add(path);
            }
            return new RowType(rowType.isNullable(), updatedFields);
        }

        @Override
        public org.apache.paimon.types.RowType project(org.apache.paimon.types.RowType rowType) {
            if (!nested) {
                return rowType.project(
                        Arrays.stream(this.projection).mapToInt(x -> x[0]).toArray());
            }

            MutableRowType result =
                    new MutableRowType(rowType.isNullable(), Collections.emptyList());
            for (int[] indexPath : this.projection) {
                org.apache.paimon.types.RowType sourceType = rowType;
                MutableRowType targetType = result;
                int index;
                for (index = 0; index < indexPath.length - 1; index++) {
                    String fieldName = sourceType.getFieldNames().get(indexPath[index]);
                    DataField field = sourceType.getField(fieldName);
                    sourceType = (org.apache.paimon.types.RowType) field.type();
                    if (!targetType.containsField(fieldName)) {
                        targetType.appendDataField(
                                fieldName,
                                field.id(),
                                new MutableRowType(
                                        sourceType.isNullable(), Collections.emptyList()),
                                field.description());
                    }
                    targetType = (MutableRowType) targetType.getField(fieldName).type();
                }

                String fieldName = sourceType.getFieldNames().get(indexPath[index]);
                DataField field = sourceType.getField(fieldName);
                targetType.appendDataField(
                        fieldName, field.id(), field.type(), field.description());
            }
            return result.toRowType();
        }

        @Override
        public ProjectionRowData getOuterProjectRow(org.apache.paimon.types.RowType rowType) {
            if (!nested) {
                return null;
            }

            org.apache.paimon.types.RowType resultType = project(rowType);

            int[][] resultIndices = new int[this.projection.length][];
            for (int i = 0; i < this.projection.length; i++) {
                org.apache.paimon.types.RowType sourceType = rowType;
                org.apache.paimon.types.RowType targetType = resultType;
                resultIndices[i] = new int[this.projection[i].length];
                for (int j = 0; j < this.projection[i].length; j++) {
                    DataField sourceField = sourceType.getFields().get(this.projection[i][j]);
                    String fieldName = sourceField.name();
                    resultIndices[i][j] = targetType.getFieldIndex(fieldName);
                    if (j < this.projection[i].length - 1) {
                        targetType =
                                (org.apache.paimon.types.RowType)
                                        targetType.getField(fieldName).type();
                        sourceType = (org.apache.paimon.types.RowType) sourceField.type();
                    }
                }
            }

            return new ProjectionRowData(toLogicalType(resultType), resultIndices);
        }

        @Override
        public boolean isNested() {
            return nested;
        }

        @Override
        public int[] toTopLevelIndexes() {
            if (isNested()) {
                throw new IllegalStateException(
                        "Cannot convert a nested projection to a top level projection");
            }
            return Arrays.stream(projection).mapToInt(arr -> arr[0]).toArray();
        }

        @Override
        public int[][] toNestedIndexes() {
            return projection;
        }
    }

    private static class TopLevelProjection extends Projection {

        final int[] projection;

        TopLevelProjection(int[] projection) {
            this.projection = projection;
        }

        @Override
        public RowType project(RowType dataType) {
            return new NestedProjection(toNestedIndexes()).project(dataType);
        }

        @Override
        public org.apache.paimon.types.RowType project(org.apache.paimon.types.RowType rowType) {
            return new NestedProjection(toNestedIndexes()).project(rowType);
        }

        @Override
        public ProjectionRowData getOuterProjectRow(org.apache.paimon.types.RowType rowType) {
            return new NestedProjection(toNestedIndexes()).getOuterProjectRow(rowType);
        }

        @Override
        public boolean isNested() {
            return false;
        }

        @Override
        public int[] toTopLevelIndexes() {
            return projection;
        }

        @Override
        public int[][] toNestedIndexes() {
            return Arrays.stream(projection).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
        }
    }

    /**
     * A mutable version of {@link org.apache.paimon.types.RowType} to facilitate the building
     * process of projections.
     *
     * <p>It is mutable in aspect of the {@link #appendDataField} method.
     */
    private static class MutableRowType extends org.apache.paimon.types.DataType {
        private final List<DataField> fields;
        private final boolean isNullable;

        private MutableRowType(org.apache.paimon.types.RowType rowType) {
            this(rowType.isNullable(), rowType.getFields());
        }

        private MutableRowType(boolean isNullable, List<DataField> fields) {
            super(isNullable, DataTypeRoot.ROW);
            this.fields = new ArrayList<>(fields);
            this.isNullable = isNullable;
        }

        private org.apache.paimon.types.RowType toRowType() {
            for (int i = 0; i < fields.size(); i++) {
                DataField field = fields.get(i);
                if (field.type() instanceof MutableRowType) {
                    fields.set(
                            i,
                            new DataField(
                                    field.id(),
                                    field.name(),
                                    ((MutableRowType) field.type()).toRowType(),
                                    field.description()));
                }
            }
            return new org.apache.paimon.types.RowType(isNullable, fields);
        }

        private boolean containsField(String fieldName) {
            for (DataField field : fields) {
                if (field.name().equals(fieldName)) {
                    return true;
                }
            }
            return false;
        }

        private DataField getField(String fieldName) {
            for (DataField field : fields) {
                if (field.name().equals(fieldName)) {
                    return field;
                }
            }

            throw new RuntimeException("Cannot find field: " + fieldName);
        }

        private void appendDataField(
                String name, int newId, org.apache.paimon.types.DataType type, String description) {
            for (DataField field : fields) {
                if (field.name().equals(name)) {
                    throw new IllegalStateException(
                            String.format(
                                    "A field with name %s has already been appended. Existing fields: %s",
                                    name, fields));
                }
                if (field.id() == newId) {
                    throw new IllegalStateException(
                            String.format(
                                    "A field with id %s has already been appended. Existing fields: %s",
                                    newId, fields));
                }
            }

            if (type instanceof org.apache.paimon.types.RowType) {
                type = new MutableRowType((org.apache.paimon.types.RowType) type);
            }
            fields.add(new DataField(newId, name, type, description));
        }

        @Override
        public int defaultSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.paimon.types.DataType copy(boolean isNullable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String asSQLString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <R> R accept(DataTypeVisitor<R> visitor) {
            throw new UnsupportedOperationException();
        }
    }
}
