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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
        List<String> preserveColumns = options.changelogEventMetadataFields();
        if (preserveColumns.isEmpty()) {
            return;
        }

        if (options.changelogProducer() != ChangelogProducer.LOOKUP) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' can only be used when '%s' is '%s', but it is '%s'.",
                            CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key(),
                            CoreOptions.CHANGELOG_PRODUCER.key(),
                            ChangelogProducer.LOOKUP,
                            options.changelogProducer()));
        }

        Set<String> valueFieldNames = new HashSet<>(valueType.getFieldNames());
        Set<String> preservedColumns = new HashSet<>();
        Set<String> metadataFieldNames = new HashSet<>();
        Map<String, String> metadataFieldSources = new HashMap<>();
        Map<String, String> storageFieldSources = new HashMap<>();
        for (String preserveColumn : preserveColumns) {
            if (!preservedColumns.add(preserveColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' is specified more than once in '%s'.",
                                preserveColumn,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }

            if (!valueFieldNames.contains(preserveColumn)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column '%s' specified in '%s' not found in value type. Available columns: %s",
                                preserveColumn,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key(),
                                valueType.getFieldNames()));
            }

            DataField physicalField = valueType.getField(preserveColumn);
            String metadataFieldName = metadataFieldName(preserveColumn, options);
            if (valueFieldNames.contains(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' created by '%s' conflicts with an existing value column.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }
            if (SpecialFields.isSystemField(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' created by '%s' conflicts with a system field.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }
            if (!metadataFieldNames.add(metadataFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' is created more than once by '%s'.",
                                metadataFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }

            String storageFieldName = storageMetadataFieldName(physicalField, options);
            if (valueFieldNames.contains(storageFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Storage metadata field '%s' created by '%s' "
                                        + "conflicts with an existing value column.",
                                storageFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }
            if (SpecialFields.isSystemField(storageFieldName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Storage metadata field '%s' created by '%s' "
                                        + "conflicts with a system field.",
                                storageFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }
            if (storageFieldSources.put(storageFieldName, preserveColumn) != null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Storage metadata field '%s' is created more than once by '%s'.",
                                storageFieldName,
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key()));
            }
            metadataFieldSources.put(metadataFieldName, preserveColumn);
        }

        for (Map.Entry<String, String> metadataField : metadataFieldSources.entrySet()) {
            String storageSource = storageFieldSources.get(metadataField.getKey());
            if (storageSource != null && !storageSource.equals(metadataField.getValue())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Metadata field '%s' created by '%s' conflicts with the storage "
                                        + "metadata field for column '%s'.",
                                metadataField.getKey(),
                                CoreOptions.CHANGELOG_PRODUCER_EVENT_METADATA_FIELDS.key(),
                                storageSource));
            }
        }
    }

    /**
     * Returns the nullable public metadata fields appended to a table row.
     *
     * <p>{@code highestFieldId} must be the table schema's highest field ID, which also covers
     * dropped fields, so that metadata field IDs never alias a field of a historical schema.
     */
    public static List<DataField> extraValueFields(
            RowType valueType, int highestFieldId, CoreOptions options) {
        validate(valueType, options);
        return metadataValueFields(
                valueType,
                highestFieldId,
                options,
                physicalField -> metadataFieldName(physicalField.name(), options));
    }

    /**
     * Returns the nullable fields used to store event metadata in changelog files.
     *
     * <p>Storage names use the configured prefix and source field ID, so they remain stable when
     * the source column is renamed.
     */
    public static List<DataField> storageValueFields(
            RowType valueType, int highestFieldId, CoreOptions options) {
        validate(valueType, options);
        return metadataValueFields(
                valueType,
                highestFieldId,
                options,
                physicalField -> storageMetadataFieldName(physicalField, options));
    }

    /**
     * Resolves storage metadata fields against the value fields of the schema a data file was
     * written with.
     *
     * <p>The metadata values were written with the source field's type at that time, so each field
     * takes the historical source field type. This lets schema evolution cast the values like the
     * source field itself. Fields whose source field did not exist yet keep the current type.
     */
    public static List<DataField> storageValueFieldsForDataSchema(
            List<DataField> storageFields,
            RowType valueType,
            List<DataField> dataValueFields,
            List<String> preserveColumns) {
        List<DataField> fields = new ArrayList<>(storageFields.size());
        for (int i = 0; i < storageFields.size(); i++) {
            DataField storageField = storageFields.get(i);
            if (i >= preserveColumns.size() || !valueType.containsField(preserveColumns.get(i))) {
                fields.add(storageField);
                continue;
            }
            int sourceFieldId = valueType.getField(preserveColumns.get(i)).id();
            DataField dataSourceField = null;
            for (DataField dataField : dataValueFields) {
                if (dataField.id() == sourceFieldId) {
                    dataSourceField = dataField;
                    break;
                }
            }
            fields.add(
                    dataSourceField == null
                            ? storageField
                            : storageField.newType(dataSourceField.type().copy(true)));
        }
        return fields;
    }

    /** Returns the physical value-field positions copied into event metadata columns. */
    @Nullable
    public static int[] preserveFieldIndices(RowType valueType, CoreOptions options) {
        List<String> preserveColumns = options.changelogEventMetadataFields();
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
            RowType baseRowType, RowType valueType, int highestFieldId, CoreOptions options) {
        return appendFields(baseRowType, extraValueFields(valueType, highestFieldId, options));
    }

    /** Appends the internal storage metadata fields to a row type. */
    public static RowType appendStorageMetadataFields(
            RowType baseRowType, RowType valueType, int highestFieldId, CoreOptions options) {
        return appendFields(baseRowType, storageValueFields(valueType, highestFieldId, options));
    }

    private static RowType appendFields(RowType baseRowType, List<DataField> extraFields) {
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

    /** Returns the public metadata field name for a preserved physical field. */
    public static String metadataFieldName(String preserveColumn, CoreOptions options) {
        return options.changelogMetadataFieldPrefix() + preserveColumn;
    }

    /** Returns the internal changelog storage name for a preserved physical field. */
    private static String storageMetadataFieldName(DataField physicalField, CoreOptions options) {
        return options.changelogMetadataFieldPrefix() + "field_id_" + physicalField.id();
    }

    private static List<DataField> metadataValueFields(
            RowType valueType,
            int highestFieldId,
            CoreOptions options,
            Function<DataField, String> metadataName) {
        List<String> preserveColumns = options.changelogEventMetadataFields();
        if (preserveColumns.isEmpty()) {
            return Collections.emptyList();
        }

        // The schema's highest field ID never decreases, so IDs above it cannot collide with
        // fields of historical schemas, including dropped fields.
        int nextId =
                Math.max(highestFieldId, RowType.currentHighestFieldId(valueType.getFields())) + 1;
        List<DataField> extraFields = new ArrayList<>(preserveColumns.size());
        for (String preserveColumn : preserveColumns) {
            DataField physicalField = valueType.getField(preserveColumn);
            extraFields.add(
                    new DataField(
                            nextId++,
                            metadataName.apply(physicalField),
                            physicalField.type().copy(true)));
        }
        return extraFields;
    }
}
