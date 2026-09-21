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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Shared validation and row-type construction for changelog event metadata fields. */
public final class ChangelogEventMetadata {

    private ChangelogEventMetadata() {}

    /**
     * Validates the event metadata configuration against the physical value type.
     *
     * <p>The fields are appended to records produced by the lookup changelog wrapper. They must not
     * be enabled for another changelog producer because those producers share the ordinary writer
     * and do not emit the appended values.
     */
    public static void validate(RowType valueType, CoreOptions options) {
        List<String> preserveColumns = options.changelogExposeFieldAsMetadata();
        if (preserveColumns.isEmpty()) {
            return;
        }

        if (options.changelogProducer() != ChangelogProducer.LOOKUP) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' can only be used when '%s' is '%s', but it is '%s'.",
                            CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key(),
                            CoreOptions.CHANGELOG_PRODUCER.key(),
                            ChangelogProducer.LOOKUP,
                            options.changelogProducer()));
        }

        Set<String> valueFieldNames = new HashSet<>(valueType.getFieldNames());
        Set<String> preservedColumns = new HashSet<>();
        Set<String> metadataFieldNames = new HashSet<>();
        for (String preserveColumn : preserveColumns) {
            if (!preservedColumns.add(preserveColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' is specified more than once in '%s'.",
                                preserveColumn,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key()));
            }

            if (!valueFieldNames.contains(preserveColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' specified in '%s' not found in value type. Available columns: %s",
                                preserveColumn,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key(),
                                valueType.getFieldNames()));
            }

            String metadataFieldName = metadataFieldName(preserveColumn, options);
            if (valueFieldNames.contains(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' created by '%s' conflicts with an existing value column.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key()));
            }
            if (SpecialFields.isSystemField(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' created by '%s' conflicts with a system field.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key()));
            }
            if (!metadataFieldNames.add(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' is created more than once by '%s'.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EXPOSE_FIELD_AS_METADATA.key()));
            }
        }
    }

    /** Returns the nullable value fields appended to a changelog value row. */
    public static List<DataField> extraValueFields(RowType valueType, CoreOptions options) {
        validate(valueType, options);
        List<String> preserveColumns = options.changelogExposeFieldAsMetadata();
        if (preserveColumns.isEmpty()) {
            return Collections.emptyList();
        }

        int nextId = valueType.getFields().stream().mapToInt(DataField::id).max().orElse(0) + 1;
        List<DataField> extraFields = new ArrayList<>(preserveColumns.size());
        for (String preserveColumn : preserveColumns) {
            DataField physicalField = valueType.getField(preserveColumn);
            extraFields.add(
                    new DataField(
                            nextId++,
                            metadataFieldName(preserveColumn, options),
                            physicalField.type().copy(true)));
        }
        return extraFields;
    }

    /** Returns the physical value-field positions copied into event metadata columns. */
    @Nullable
    public static int[] preserveFieldIndices(RowType valueType, CoreOptions options) {
        List<String> preserveColumns = options.changelogExposeFieldAsMetadata();
        if (preserveColumns.isEmpty()) {
            return null;
        }
        validate(valueType, options);
        int[] indices = new int[preserveColumns.size()];
        for (int i = 0; i < preserveColumns.size(); i++) {
            indices[i] = valueType.getFieldIndex(preserveColumns.get(i));
        }
        return indices;
    }

    /**
     * Appends event metadata fields to a row type while resolving their source fields from the
     * physical value type.
     *
     * <p>The two row types intentionally differ for system tables such as {@code audit_log}, whose
     * base row prepends system fields to the physical value fields.
     */
    public static RowType appendMetadataFields(
            RowType baseRowType, RowType valueType, CoreOptions options) {
        List<DataField> extraFields = extraValueFields(valueType, options);
        if (extraFields.isEmpty()) {
            return baseRowType;
        }

        boolean hasExistingMetadata = false;
        for (DataField extraField : extraFields) {
            if (baseRowType.containsField(extraField.name())) {
                hasExistingMetadata = true;
            }
        }
        if (hasExistingMetadata) {
            boolean allExisting =
                    extraFields.stream().allMatch(field -> baseRowType.containsField(field.name()));
            if (allExisting) {
                return baseRowType;
            }
            throw new IllegalArgumentException(
                    "Changelog event metadata fields conflict with the requested row type.");
        }

        List<DataField> fields = new ArrayList<>(baseRowType.getFields());
        fields.addAll(extraFields);
        return new RowType(fields);
    }

    /** Returns the generated field name for a preserved physical field. */
    public static String metadataFieldName(String preserveColumn, CoreOptions options) {
        return options.changelogMetadataFieldPrefix() + preserveColumn;
    }
}
