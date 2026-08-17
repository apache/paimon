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

package org.apache.paimon.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A resolved path from a root {@link RowType} to one of its ROW-nested fields.
 *
 * <p>Collection elements and map keys or values are not traversed.
 */
public final class ResolvedFieldPath implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<DataField> fields;
    private final int[] indexes;

    private ResolvedFieldPath(List<DataField> fields, int[] indexes) {
        if (fields.isEmpty() || fields.size() != indexes.length) {
            throw new IllegalArgumentException("Fields and indexes must form a non-empty path.");
        }
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.indexes = Arrays.copyOf(indexes, indexes.length);
    }

    /**
     * Resolves a field name. An exact top-level field name takes precedence over interpreting dots
     * as nested path separators.
     */
    public static Optional<ResolvedFieldPath> resolve(RowType rowType, String fieldName) {
        Objects.requireNonNull(rowType, "Row type must not be null.");
        Objects.requireNonNull(fieldName, "Field name must not be null.");

        if (fieldName.isEmpty()) {
            return Optional.empty();
        }

        int topLevelIndex = rowType.getFieldIndex(fieldName);
        if (topLevelIndex >= 0) {
            return Optional.of(
                    new ResolvedFieldPath(
                            Collections.singletonList(rowType.getFields().get(topLevelIndex)),
                            new int[] {topLevelIndex}));
        }

        return resolve(rowType, Arrays.asList(fieldName.split("\\.", -1)));
    }

    /** Resolves a field path whose elements are unambiguous field names. */
    public static Optional<ResolvedFieldPath> resolve(RowType rowType, List<String> fieldNames) {
        Objects.requireNonNull(rowType, "Row type must not be null.");
        Objects.requireNonNull(fieldNames, "Field names must not be null.");

        if (fieldNames.isEmpty()) {
            return Optional.empty();
        }

        List<DataField> fields = new ArrayList<>(fieldNames.size());
        int[] indexes = new int[fieldNames.size()];
        RowType currentRowType = rowType;
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            if (fieldName == null || fieldName.isEmpty()) {
                return Optional.empty();
            }

            int index = currentRowType.getFieldIndex(fieldName);
            if (index < 0) {
                return Optional.empty();
            }

            DataField field = currentRowType.getFields().get(index);
            fields.add(field);
            indexes[i] = index;

            if (i < fieldNames.size() - 1) {
                if (!(field.type() instanceof RowType)) {
                    return Optional.empty();
                }
                currentRowType = (RowType) field.type();
            }
        }

        return Optional.of(new ResolvedFieldPath(fields, indexes));
    }

    /** Resolves a field ID by traversing nested {@link RowType}s. */
    public static Optional<ResolvedFieldPath> resolve(RowType rowType, int fieldId) {
        Objects.requireNonNull(rowType, "Row type must not be null.");
        return resolveById(rowType, fieldId);
    }

    /** Resolves field paths in their declared order. */
    public static Optional<List<ResolvedFieldPath>> resolveAll(
            RowType rowType, List<String> fieldPaths) {
        Objects.requireNonNull(rowType, "Row type must not be null.");
        Objects.requireNonNull(fieldPaths, "Field paths must not be null.");

        List<ResolvedFieldPath> resolved = new ArrayList<>(fieldPaths.size());
        for (String fieldPath : fieldPaths) {
            Optional<ResolvedFieldPath> path = resolve(rowType, fieldPath);
            if (!path.isPresent()) {
                return Optional.empty();
            }
            resolved.add(path.get());
        }
        return Optional.of(Collections.unmodifiableList(resolved));
    }

    /**
     * Projects the distinct top-level fields needed to read the supplied paths, preserving their
     * first-use order. All paths must have been resolved from {@code rowType}.
     */
    public static RowType projectTopLevel(
            RowType rowType, List<ResolvedFieldPath> resolvedFieldPaths) {
        Objects.requireNonNull(rowType, "Row type must not be null.");
        Objects.requireNonNull(resolvedFieldPaths, "Resolved field paths must not be null.");

        Set<Integer> indexes = new LinkedHashSet<>();
        for (ResolvedFieldPath path : resolvedFieldPaths) {
            indexes.add(path.topLevelIndex());
        }
        int[] projection = new int[indexes.size()];
        int pos = 0;
        for (Integer index : indexes) {
            projection[pos++] = index;
        }
        return rowType.project(projection);
    }

    private static Optional<ResolvedFieldPath> resolveById(RowType rowType, int fieldId) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (field.id() == fieldId) {
                return Optional.of(
                        new ResolvedFieldPath(Collections.singletonList(field), new int[] {i}));
            }

            if (field.type() instanceof RowType) {
                Optional<ResolvedFieldPath> nested = resolveById((RowType) field.type(), fieldId);
                if (nested.isPresent()) {
                    return Optional.of(nested.get().prepend(field, i));
                }
            }
        }
        return Optional.empty();
    }

    private ResolvedFieldPath prepend(DataField field, int index) {
        List<DataField> prependedFields = new ArrayList<>(fields.size() + 1);
        prependedFields.add(field);
        prependedFields.addAll(fields);

        int[] prependedIndexes = new int[indexes.length + 1];
        prependedIndexes[0] = index;
        System.arraycopy(indexes, 0, prependedIndexes, 1, indexes.length);
        return new ResolvedFieldPath(prependedFields, prependedIndexes);
    }

    /** Returns all fields in the path, including the top-level field and leaf field. */
    public List<DataField> fields() {
        return fields;
    }

    /** Returns all field names in the path as its unambiguous structured representation. */
    public List<String> fieldNames() {
        List<String> names = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            names.add(field.name());
        }
        return Collections.unmodifiableList(names);
    }

    /** Returns all ordinal positions in the path, including the top-level position. */
    public int[] indexes() {
        return Arrays.copyOf(indexes, indexes.length);
    }

    /** Returns the top-level ordinal position. */
    public int topLevelIndex() {
        return indexes[0];
    }

    /** Returns ordinal positions below the top-level row. */
    public int[] nestedIndexes() {
        return Arrays.copyOfRange(indexes, 1, indexes.length);
    }

    /** Returns the arity of each row traversed below the top-level field. */
    public int[] nestedArities() {
        int[] arities = new int[fields.size() - 1];
        for (int i = 0; i < arities.length; i++) {
            arities[i] = ((RowType) fields.get(i).type()).getFieldCount();
        }
        return arities;
    }

    /** Returns the top-level field. */
    public DataField topLevelField() {
        return fields.get(0);
    }

    /** Returns the leaf field. */
    public DataField leafField() {
        return fields.get(fields.size() - 1);
    }

    /** Returns whether this path traverses at least one nested row. */
    public boolean isNested() {
        return fields.size() > 1;
    }

    /** Returns the dot-separated path. */
    public String fullName() {
        StringBuilder builder = new StringBuilder();
        for (DataField field : fields) {
            if (builder.length() > 0) {
                builder.append('.');
            }
            builder.append(field.name());
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResolvedFieldPath)) {
            return false;
        }
        ResolvedFieldPath that = (ResolvedFieldPath) o;
        return fields.equals(that.fields) && Arrays.equals(indexes, that.indexes);
    }

    @Override
    public int hashCode() {
        return 31 * fields.hashCode() + Arrays.hashCode(indexes);
    }

    @Override
    public String toString() {
        return fullName();
    }
}
