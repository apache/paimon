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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Data type of sequence of fields. A field consists of a field name, field type, and an optional
 * description. The most specific type of a row of a table is a row type. In this case, each column
 * of the row corresponds to the field of the row type that has the same ordinal position as the
 * column. Compared to the SQL standard, an optional field description simplifies the handling with
 * complex structures.
 *
 * @since 0.4.0
 */
@Public
public final class RowType extends DataType {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FIELDS = "fields";

    public static final String FORMAT = "ROW<%s>";

    private final List<DataField> fields;

    private transient volatile Map<String, DataField> laziedNameToField;
    private transient volatile Map<String, Integer> laziedNameToIndex;

    private transient volatile Map<Integer, DataField> laziedFieldIdToField;
    private transient volatile Map<Integer, Integer> laziedFieldIdToIndex;

    public RowType(boolean isNullable, List<DataField> fields) {
        super(isNullable, DataTypeRoot.ROW);
        this.fields =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(fields, "Fields must not be null.")));

        validateFields(fields);
    }

    @JsonCreator
    public RowType(@JsonProperty(FIELD_FIELDS) List<DataField> fields) {
        this(true, fields);
    }

    public RowType copy(List<DataField> newFields) {
        return new RowType(isNullable(), newFields);
    }

    public List<DataField> getFields() {
        return fields;
    }

    public List<String> getFieldNames() {
        return fields.stream().map(DataField::name).collect(Collectors.toList());
    }

    public List<DataType> getFieldTypes() {
        return fields.stream().map(DataField::type).collect(Collectors.toList());
    }

    public DataType getTypeAt(int i) {
        return fields.get(i).type();
    }

    public int getFieldCount() {
        return fields.size();
    }

    public int getFieldIndex(String fieldName) {
        return nameToIndex().getOrDefault(fieldName, -1);
    }

    public int[] getFieldIndices(List<String> projectFields) {
        int[] projection = new int[projectFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = getFieldIndex(projectFields.get(i));
        }
        return projection;
    }

    public boolean containsField(String fieldName) {
        return nameToField().containsKey(fieldName);
    }

    public boolean containsField(int fieldId) {
        return fieldIdToField().containsKey(fieldId);
    }

    public boolean notContainsField(String fieldName) {
        return !containsField(fieldName);
    }

    public DataField getField(String fieldName) {
        DataField field = nameToField().get(fieldName);
        if (field == null) {
            throw new RuntimeException("Cannot find field: " + fieldName);
        }
        return field;
    }

    public DataField getField(int fieldId) {
        DataField field = fieldIdToField().get(fieldId);
        if (field == null) {
            throw new RuntimeException("Cannot find field by field id: " + fieldId);
        }
        return field;
    }

    public int getFieldIndexByFieldId(int fieldId) {
        Integer index = fieldIdToIndex().get(fieldId);
        if (index == null) {
            throw new RuntimeException("Cannot find field index by FieldId " + fieldId);
        }
        return index;
    }

    @Override
    public int defaultSize() {
        return fields.stream().mapToInt(f -> f.type().defaultSize()).sum();
    }

    @Override
    public RowType copy(boolean isNullable) {
        return new RowType(
                isNullable, fields.stream().map(DataField::copy).collect(Collectors.toList()));
    }

    @Override
    public RowType notNull() {
        return copy(false);
    }

    @Override
    public String asSQLString() {
        return withNullability(
                FORMAT,
                fields.stream().map(DataField::asSQLString).collect(Collectors.joining(", ")));
    }

    @Override
    public void serializeJson(JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        generator.writeStringField("type", isNullable() ? "ROW" : "ROW NOT NULL");
        generator.writeArrayFieldStart("fields");
        for (DataField field : getFields()) {
            field.serializeJson(generator);
        }
        generator.writeEndArray();
        generator.writeEndObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        return fields.equals(rowType.fields);
    }

    @Override
    public boolean equalsIgnoreFieldId(DataType o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType other = (RowType) o;
        if (fields.size() != other.fields.size()) {
            return false;
        }
        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).equalsIgnoreFieldId(other.fields.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isPrunedFrom(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowType rowType = (RowType) o;
        for (DataField field : fields) {
            if (!field.isPrunedFrom(rowType.getField(field.id()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }

    private static void validateFields(List<DataField> fields) {
        final List<String> fieldNames =
                fields.stream().map(DataField::name).collect(Collectors.toList());
        if (fieldNames.stream().anyMatch(StringUtils::isNullOrWhitespaceOnly)) {
            throw new IllegalArgumentException(
                    "Field names must contain at least one non-whitespace character.");
        }
        final Set<String> duplicates = Schema.duplicateFields(fieldNames);

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("Field names must be unique. Found duplicates: %s", duplicates));
        }
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public void collectFieldIds(Set<Integer> fieldIds) {
        for (DataField field : fields) {
            if (fieldIds.contains(field.id())) {
                throw new RuntimeException(
                        String.format("Broken schema, field id %s is duplicated.", field.id()));
            }
            fieldIds.add(field.id());
            field.type().collectFieldIds(fieldIds);
        }
    }

    public RowType project(int[] mapping) {
        List<DataField> fields = getFields();
        return new RowType(
                        Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()))
                .copy(isNullable());
    }

    public RowType project(List<String> names) {
        List<DataField> fields = getFields();
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return new RowType(
                        names.stream()
                                .map(k -> fields.get(fieldNames.indexOf(k)))
                                .collect(Collectors.toList()))
                .copy(isNullable());
    }

    public int[] projectIndexes(List<String> names) {
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return names.stream().mapToInt(fieldNames::indexOf).toArray();
    }

    public RowType project(String... names) {
        return project(Arrays.asList(names));
    }

    /**
     * Project this row type by a list of (possibly nested) dotted paths, e.g. {@code ["f0",
     * "nest.a"]}. A path without a dot selects the whole top-level field (same as {@link
     * #project(List)}); a dotted path selects only the addressed sub-field of a nested {@link
     * RowType}, preserving field ids and nullability of every level. Schema field order is
     * preserved. This is used by data evolution to reconstruct the partial nested schema of a
     * column-group file from its {@code writeCols}.
     */
    public RowType projectByPaths(List<String> paths) {
        return projectTypeByPaths(this, paths);
    }

    private static RowType projectTypeByPaths(RowType type, List<String> paths) {
        // group paths by their immediate child name; a child appearing without a tail (or also with
        // a tail) is selected as a whole field
        Map<String, List<String>> childToSubPaths = new HashMap<>();
        Set<String> wholeChildren = new HashSet<>();
        Set<String> fieldNames = new HashSet<>();
        for (DataField field : type.getFields()) {
            fieldNames.add(field.name());
        }
        for (String path : paths) {
            int dot = path.indexOf('.');
            // Prefer an exact field-name match so a column whose name itself contains a dot (and
            // any
            // plain top-level name) is selected whole; only split into head.tail for genuine nested
            // sub-field paths that do not name a field directly. This keeps backward compatibility
            // with the legacy exact-name project(List).
            if (dot < 0 || fieldNames.contains(path)) {
                childToSubPaths.computeIfAbsent(path, k -> new ArrayList<>());
                wholeChildren.add(path);
            } else {
                String head = path.substring(0, dot);
                String tail = path.substring(dot + 1);
                childToSubPaths.computeIfAbsent(head, k -> new ArrayList<>()).add(tail);
            }
        }

        Set<String> matched = new HashSet<>();
        List<DataField> result = new ArrayList<>();
        for (DataField field : type.getFields()) {
            List<String> subPaths = childToSubPaths.get(field.name());
            if (subPaths == null) {
                continue;
            }
            matched.add(field.name());
            if (wholeChildren.contains(field.name()) || subPaths.isEmpty()) {
                result.add(field);
            } else if (field.type() instanceof RowType) {
                RowType prunedChild =
                        projectTypeByPaths((RowType) field.type(), subPaths)
                                .copy(field.type().isNullable());
                result.add(field.newType(prunedChild));
            } else {
                // a dotted path addresses a sub-field, but this field is not a ROW; reject rather
                // than silently selecting the whole field, so invalid dotted paths surface early
                throw new IllegalArgumentException(
                        "Cannot project sub-field(s) "
                                + subPaths
                                + " of non-ROW field '"
                                + field.name()
                                + "' in "
                                + type);
            }
        }
        if (!matched.containsAll(childToSubPaths.keySet())) {
            Set<String> unknown = new HashSet<>(childToSubPaths.keySet());
            unknown.removeAll(matched);
            throw new IllegalArgumentException(
                    "Cannot project by paths, unknown field(s) " + unknown + " in " + type);
        }
        return new RowType(type.isNullable(), result);
    }

    /**
     * Compute the dotted paths describing this (possibly partially nested) write type relative to a
     * full row type. A top-level field, or a nested field whose structure fully covers the
     * corresponding field in {@code fullType}, is emitted by its name; a nested field that only
     * covers some sub-fields is expanded into dotted leaf paths. This is the inverse of {@link
     * #projectByPaths(List)} and is used to derive {@code writeCols}.
     */
    public List<String> leafPaths(RowType fullType) {
        List<String> result = new ArrayList<>();
        collectLeafPaths(getFields(), fullType, "", result);
        return result;
    }

    private static void collectLeafPaths(
            List<DataField> writeFields, RowType fullType, String prefix, List<String> out) {
        for (DataField writeField : writeFields) {
            String path = prefix.isEmpty() ? writeField.name() : prefix + "." + writeField.name();
            // A field absent from the reference type (e.g. the _ROW_ID / _SEQUENCE_NUMBER special
            // fields added by row tracking, which are not part of the table's logical row type) has
            // no sub-field split: emit it whole by name, matching the legacy getFieldNames()
            // output.
            if (!fullType.containsField(writeField.id())) {
                out.add(path);
                continue;
            }
            DataField fullField = fullType.getField(writeField.id());
            boolean willExpand =
                    writeField.type() instanceof RowType
                            && fullField.type() instanceof RowType
                            && !coversFully(
                                    (RowType) writeField.type(), (RowType) fullField.type());
            // A dotted path is only unambiguous if no name segment contains a literal '.'. A name
            // with a dot is fine when emitted whole at top level (projectByPaths matches it
            // exactly),
            // but not when it participates in a multi-segment nested path.
            if (writeField.name().indexOf('.') >= 0 && (!prefix.isEmpty() || willExpand)) {
                throw new UnsupportedOperationException(
                        "Sub-field-level data evolution does not support a nested field whose name "
                                + "contains '.': "
                                + path);
            }
            if (willExpand) {
                // A partial struct nested inside another partial struct (a path deeper than one
                // level, e.g. nest.sub.x) cannot be composed back on read — the data-evolution read
                // path only assembles one nested level. Reject it here so such a file is never
                // written/committed and later breaks full-table reads.
                if (!prefix.isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Sub-field-level data evolution supports only one level of partial "
                                    + "nesting; the nested sub-field '"
                                    + path
                                    + "' cannot be partially written. Write the whole '"
                                    + path
                                    + "' sub-field instead.");
                }
                collectLeafPaths(
                        ((RowType) writeField.type()).getFields(),
                        (RowType) fullField.type(),
                        path,
                        out);
            } else {
                out.add(path);
            }
        }
    }

    /** Whether {@code part} contains every (recursively nested) field of {@code full}. */
    private static boolean coversFully(RowType part, RowType full) {
        if (part.getFieldCount() != full.getFieldCount()) {
            return false;
        }
        for (DataField fullField : full.getFields()) {
            if (!part.containsField(fullField.id())) {
                return false;
            }
            DataField partField = part.getField(fullField.id());
            if (partField.type() instanceof RowType && fullField.type() instanceof RowType) {
                if (!coversFully((RowType) partField.type(), (RowType) fullField.type())) {
                    return false;
                }
            }
        }
        return true;
    }

    private Map<String, DataField> nameToField() {
        Map<String, DataField> nameToField = this.laziedNameToField;
        if (nameToField == null) {
            nameToField = new HashMap<>();
            for (DataField field : fields) {
                nameToField.put(field.name(), field);
            }
            this.laziedNameToField = nameToField;
        }
        return nameToField;
    }

    private Map<String, Integer> nameToIndex() {
        Map<String, Integer> nameToIndex = this.laziedNameToIndex;
        if (nameToIndex == null) {
            nameToIndex = new HashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                nameToIndex.put(fields.get(i).name(), i);
            }
            this.laziedNameToIndex = nameToIndex;
        }
        return nameToIndex;
    }

    private Map<Integer, DataField> fieldIdToField() {
        Map<Integer, DataField> fieldIdToField = this.laziedFieldIdToField;
        if (fieldIdToField == null) {
            fieldIdToField = new HashMap<>();
            for (DataField field : fields) {
                fieldIdToField.put(field.id(), field);
            }
            this.laziedFieldIdToField = fieldIdToField;
        }
        return fieldIdToField;
    }

    private Map<Integer, Integer> fieldIdToIndex() {
        Map<Integer, Integer> fieldIdToIndex = this.laziedFieldIdToIndex;
        if (fieldIdToIndex == null) {
            fieldIdToIndex = new HashMap<>();
            for (int i = 0; i < fields.size(); i++) {
                fieldIdToIndex.put(fields.get(i).id(), i);
            }
            this.laziedFieldIdToIndex = fieldIdToIndex;
        }
        return fieldIdToIndex;
    }

    public static RowType of() {
        return new RowType(true, Collections.emptyList());
    }

    public static RowType of(DataField... fields) {
        final List<DataField> fs = new ArrayList<>(Arrays.asList(fields));
        return new RowType(true, fs);
    }

    public static RowType of(DataType... types) {
        final List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, "f" + i, types[i]));
        }
        return new RowType(true, fields);
    }

    public static RowType of(DataType[] types, String[] names) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(new DataField(i, names[i], types[i]));
        }
        return new RowType(true, fields);
    }

    public static int currentHighestFieldId(List<DataField> fields) {
        Set<Integer> fieldIds = new HashSet<>();
        new RowType(fields).collectFieldIds(fieldIds);
        return fieldIds.stream()
                .filter(i -> !SpecialFields.isSystemField(i))
                .max(Integer::compareTo)
                .orElse(-1);
    }

    public static Builder builder() {
        return builder(new AtomicInteger(-1));
    }

    public static Builder builder(AtomicInteger fieldId) {
        return builder(true, fieldId);
    }

    public static Builder builder(boolean isNullable, AtomicInteger fieldId) {
        return new Builder(isNullable, fieldId);
    }

    /** Builder of {@link RowType}. */
    public static class Builder {

        private final List<DataField> fields = new ArrayList<>();

        private final boolean isNullable;
        private final AtomicInteger fieldId;

        private Builder(boolean isNullable, AtomicInteger fieldId) {
            this.isNullable = isNullable;
            this.fieldId = fieldId;
        }

        public Builder field(String name, DataType type) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type));
            return this;
        }

        public Builder field(String name, DataType type, @Nullable String description) {
            fields.add(new DataField(fieldId.incrementAndGet(), name, type, description));
            return this;
        }

        public Builder field(
                String name,
                DataType type,
                @Nullable String description,
                @Nullable String defaultValue) {
            fields.add(
                    new DataField(
                            fieldId.incrementAndGet(), name, type, description, defaultValue));
            return this;
        }

        public Builder fields(List<DataType> types) {
            for (int i = 0; i < types.size(); i++) {
                field("f" + i, types.get(i));
            }
            return this;
        }

        public Builder fields(DataType... types) {
            for (int i = 0; i < types.length; i++) {
                field("f" + i, types[i]);
            }
            return this;
        }

        public Builder fields(DataType[] types, String[] names) {
            for (int i = 0; i < types.length; i++) {
                field(names[i], types[i]);
            }
            return this;
        }

        public RowType build() {
            return new RowType(isNullable, fields);
        }
    }
}
