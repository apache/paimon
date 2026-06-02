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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.FallbackKey;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utilities for column comment directives (BLOB / VECTOR type conversion via ADD COLUMN). */
public final class ColumnDirectiveUtils {

    public static final String BLOB_FIELD_DIRECTIVE = "__BLOB_FIELD";
    public static final String BLOB_DESCRIPTOR_FIELD_DIRECTIVE = "__BLOB_DESCRIPTOR_FIELD";
    public static final String BLOB_VIEW_FIELD_DIRECTIVE = "__BLOB_VIEW_FIELD";
    public static final String BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE =
            "__BLOB_EXTERNAL_STORAGE_FIELD";
    public static final String VECTOR_FIELD_DIRECTIVE = "__VECTOR_FIELD";

    private ColumnDirectiveUtils() {}

    /**
     * Parses the comment of an {@code ALTER TABLE ADD COLUMN} statement. Returns {@code null} when
     * the comment is a regular user comment; returns a {@link ParsedDirective} when the comment
     * begins with a supported directive. Throws {@link IllegalArgumentException} when the comment
     * begins with {@code __BLOB} or {@code __VECTOR} but is not one of the supported directives.
     */
    @Nullable
    public static ParsedDirective parseAddColumnComment(@Nullable String comment) {
        if (comment == null) {
            return null;
        }
        comment = StringUtils.trim(comment);
        if (comment.startsWith("__VECTOR")) {
            return parseVectorDirective(comment);
        }
        if (!comment.startsWith("__BLOB")) {
            return null;
        }
        // match longer prefixes first
        String optionKey = matchDirective(comment, BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE);
        String marker = BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE;
        if (optionKey == null) {
            optionKey = matchDirective(comment, BLOB_VIEW_FIELD_DIRECTIVE);
            marker = BLOB_VIEW_FIELD_DIRECTIVE;
        }
        if (optionKey == null) {
            optionKey = matchDirective(comment, BLOB_DESCRIPTOR_FIELD_DIRECTIVE);
            marker = BLOB_DESCRIPTOR_FIELD_DIRECTIVE;
        }
        if (optionKey == null) {
            optionKey = matchDirective(comment, BLOB_FIELD_DIRECTIVE);
            marker = BLOB_FIELD_DIRECTIVE;
        }
        Preconditions.checkArgument(
                optionKey != null,
                "Unsupported BLOB directive in column comment: '%s'. Supported directives are "
                        + "'%s', '%s', '%s' and '%s'.",
                comment,
                BLOB_FIELD_DIRECTIVE,
                BLOB_DESCRIPTOR_FIELD_DIRECTIVE,
                BLOB_VIEW_FIELD_DIRECTIVE,
                BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE);
        String realComment =
                comment.length() == marker.length()
                        ? null
                        : comment.substring(marker.length() + 1).trim();
        if (realComment != null && realComment.isEmpty()) {
            realComment = null;
        }
        return new ParsedDirective(optionKey, realComment, false, 0);
    }

    private static ParsedDirective parseVectorDirective(String comment) {
        String optionKey = matchDirective(comment, VECTOR_FIELD_DIRECTIVE);
        Preconditions.checkArgument(
                optionKey != null,
                "Unsupported VECTOR directive in column comment: '%s'. Supported directive is '%s'.",
                comment,
                VECTOR_FIELD_DIRECTIVE);
        if (comment.length() == VECTOR_FIELD_DIRECTIVE.length()) {
            throw new IllegalArgumentException(
                    String.format(
                            "VECTOR directive '%s' requires a dimension, e.g. '%s;128' or '%s;128; my comment'.",
                            comment, VECTOR_FIELD_DIRECTIVE, VECTOR_FIELD_DIRECTIVE));
        }
        String rest = comment.substring(VECTOR_FIELD_DIRECTIVE.length() + 1);
        int dimEnd = rest.indexOf(';');
        String dimStr;
        String realComment;
        if (dimEnd < 0) {
            dimStr = rest.trim();
            realComment = null;
        } else {
            dimStr = rest.substring(0, dimEnd).trim();
            realComment = rest.substring(dimEnd + 1).trim();
            if (realComment.isEmpty()) {
                realComment = null;
            }
        }
        int dim;
        try {
            dim = Integer.parseInt(dimStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected an integer dimension after '%s;', but got: '%s'.",
                            VECTOR_FIELD_DIRECTIVE, dimStr));
        }
        Preconditions.checkArgument(dim >= 1, "Vector dimension must be >= 1, but got: %s.", dim);
        return new ParsedDirective(optionKey, realComment, true, dim);
    }

    @Nullable
    private static String matchDirective(String comment, String marker) {
        if (!comment.startsWith(marker)) {
            return null;
        }
        if (comment.length() == marker.length()) {
            return optionKeyFor(marker);
        }
        return comment.charAt(marker.length()) == ';' ? optionKeyFor(marker) : null;
    }

    private static String optionKeyFor(String marker) {
        if (BLOB_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_FIELD.key();
        } else if (BLOB_DESCRIPTOR_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_DESCRIPTOR_FIELD.key();
        } else if (BLOB_VIEW_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_VIEW_FIELD.key();
        } else if (BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key();
        } else if (VECTOR_FIELD_DIRECTIVE.equals(marker)) {
            return CoreOptions.VECTOR_FIELD.key();
        } else {
            throw new IllegalArgumentException("Unsupported directive: " + marker);
        }
    }

    /**
     * One-stop method for ADD COLUMN: parses the comment, converts the type, and appends the field
     * to the corresponding option. Returns {@code null} when the comment is not a directive.
     */
    @Nullable
    public static ConvertedColumn applyAddColumnDirective(
            @Nullable String comment,
            String fieldName,
            DataType sourceType,
            Map<String, String> options) {
        ParsedDirective directive = parseAddColumnComment(comment);
        if (directive == null) {
            return null;
        }
        DataType newType = convertType(directive, fieldName, sourceType);
        modifyFieldOptions(directive.optionKey(), fieldName, options);
        if (CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key().equals(directive.optionKey())) {
            modifyFieldOptions(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), fieldName, options);
        }
        return new ConvertedColumn(newType, directive.realComment());
    }

    /**
     * Process comment directives on every field of a {@link Schema} used for CREATE TABLE. Fields
     * whose comment matches a directive get their type converted and the corresponding option
     * appended; the directive prefix is stripped from the stored comment.
     */
    public static Schema applyDirectives(Schema schema) {
        List<DataField> fields = schema.fields();
        Map<String, String> options = new HashMap<>(schema.options());
        List<DataField> newFields = new ArrayList<>(fields.size());
        boolean changed = false;

        for (DataField field : fields) {
            ConvertedColumn converted =
                    applyAddColumnDirective(
                            field.description(), field.name(), field.type(), options);
            if (converted == null) {
                newFields.add(field);
            } else {
                changed = true;
                newFields.add(
                        new DataField(
                                field.id(), field.name(), converted.type(), converted.comment()));
            }
        }

        if (!changed) {
            return schema;
        }
        return new Schema(
                newFields, schema.partitionKeys(), schema.primaryKeys(), options, schema.comment());
    }

    private static DataType convertType(
            ParsedDirective directive, String fieldName, DataType sourceType) {
        if (directive.isVector()) {
            Preconditions.checkArgument(
                    sourceType.getTypeRoot() == DataTypeRoot.ARRAY,
                    "Column %s declared with a VECTOR directive must be of ARRAY type, but was %s.",
                    fieldName,
                    sourceType);
            DataType elementType = ((ArrayType) sourceType).getElementType();
            return new VectorType(sourceType.isNullable(), directive.vectorDim(), elementType);
        } else {
            DataTypeRoot root = sourceType.getTypeRoot();
            Preconditions.checkArgument(
                    root == DataTypeRoot.VARBINARY
                            || root == DataTypeRoot.BINARY
                            || root == DataTypeRoot.BLOB,
                    "Column %s declared with a BLOB directive must be of BYTES, "
                            + "BINARY or BLOB type, but was %s.",
                    fieldName,
                    sourceType);
            return new BlobType(sourceType.isNullable());
        }
    }

    /**
     * Append {@code fieldName} to the comma-separated option identified by {@code optionKey}. If
     * the canonical key is empty but a fallback key holds the value (e.g. legacy {@code
     * blob.stored-descriptor-fields}), the fallback value is migrated to the canonical key before
     * appending so old entries are not shadowed.
     */
    public static void modifyFieldOptions(
            String optionKey, String fieldName, Map<String, String> options) {
        ConfigOption<String> option;
        if (CoreOptions.BLOB_FIELD.key().equals(optionKey)) {
            option = CoreOptions.BLOB_FIELD;
        } else if (CoreOptions.BLOB_DESCRIPTOR_FIELD.key().equals(optionKey)) {
            option = CoreOptions.BLOB_DESCRIPTOR_FIELD;
        } else if (CoreOptions.BLOB_VIEW_FIELD.key().equals(optionKey)) {
            option = CoreOptions.BLOB_VIEW_FIELD;
        } else if (CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key().equals(optionKey)) {
            option = CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD;
        } else if (CoreOptions.VECTOR_FIELD.key().equals(optionKey)) {
            option = CoreOptions.VECTOR_FIELD;
        } else {
            throw new IllegalArgumentException("Unsupported directive: " + optionKey);
        }

        String existing = options.get(optionKey);
        if (existing == null || existing.isEmpty()) {
            for (FallbackKey fk : option.fallbackKeys()) {
                String fallbackValue = options.remove(fk.getKey());
                if (fallbackValue != null && !fallbackValue.isEmpty()) {
                    existing = fallbackValue;
                    break;
                }
            }
        }
        String newValue = existing == null ? fieldName : existing + "," + fieldName;
        options.put(optionKey, newValue);
    }

    private static final ConfigOption<String>[] BLOB_OPTIONS =
            new ConfigOption[] {
                CoreOptions.BLOB_FIELD,
                CoreOptions.BLOB_DESCRIPTOR_FIELD,
                CoreOptions.BLOB_VIEW_FIELD,
                CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD
            };

    private static final ConfigOption<String>[] VECTOR_OPTIONS =
            new ConfigOption[] {CoreOptions.VECTOR_FIELD};

    /**
     * Remove directive-managed options when a BLOB or VECTOR column is dropped. Only acts on BLOB
     * or VECTOR type columns; other types are ignored.
     */
    public static void removeDroppedDirectiveOptions(
            String fieldName, DataTypeRoot typeRoot, Map<String, String> options) {
        if (typeRoot == DataTypeRoot.BLOB) {
            for (ConfigOption<String> option : BLOB_OPTIONS) {
                removeFromCsvOption(option.key(), fieldName, options);
                for (FallbackKey fk : option.fallbackKeys()) {
                    removeFromCsvOption(fk.getKey(), fieldName, options);
                }
            }
        } else if (typeRoot == DataTypeRoot.VECTOR) {
            for (ConfigOption<String> option : VECTOR_OPTIONS) {
                removeFromCsvOption(option.key(), fieldName, options);
                for (FallbackKey fk : option.fallbackKeys()) {
                    removeFromCsvOption(fk.getKey(), fieldName, options);
                }
            }
            options.remove(String.format("field.%s.vector-dim", fieldName));
        }
    }

    private static void removeFromCsvOption(
            String key, String fieldName, Map<String, String> options) {
        String existing = options.get(key);
        if (existing == null || existing.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (String v : existing.split(",")) {
            String trimmed = v.trim();
            if (trimmed.isEmpty() || trimmed.equals(fieldName)) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(trimmed);
        }
        if (sb.length() == 0) {
            options.remove(key);
        } else {
            options.put(key, sb.toString());
        }
    }

    /** Result of {@link #applyAddColumnDirective}: the converted type and effective comment. */
    public static final class ConvertedColumn {
        private final DataType type;
        @Nullable private final String comment;

        private ConvertedColumn(DataType type, @Nullable String comment) {
            this.type = type;
            this.comment = comment;
        }

        public DataType type() {
            return type;
        }

        @Nullable
        public String comment() {
            return comment;
        }
    }

    /** Parsed directive: the option key to update, user-facing comment, and vector metadata. */
    public static final class ParsedDirective {
        private final String optionKey;
        @Nullable private final String realComment;
        private final boolean isVector;
        private final int vectorDim;

        private ParsedDirective(
                String optionKey, @Nullable String realComment, boolean isVector, int vectorDim) {
            this.optionKey = optionKey;
            this.realComment = realComment;
            this.isVector = isVector;
            this.vectorDim = vectorDim;
        }

        public String optionKey() {
            return optionKey;
        }

        @Nullable
        public String realComment() {
            return realComment;
        }

        public boolean isVector() {
            return isVector;
        }

        public int vectorDim() {
            return vectorDim;
        }
    }
}
