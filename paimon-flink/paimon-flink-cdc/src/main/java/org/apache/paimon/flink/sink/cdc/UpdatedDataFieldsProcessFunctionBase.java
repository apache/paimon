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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.schema.NestedSchemaUtils;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.FieldIdentifier;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Base class for update data fields process function. */
public abstract class UpdatedDataFieldsProcessFunctionBase<I, O> extends ProcessFunction<I, O> {

    private static final Logger LOG =
            LoggerFactory.getLogger(UpdatedDataFieldsProcessFunctionBase.class);

    protected final CatalogLoader catalogLoader;
    private final TypeMapping typeMapping;

    protected Catalog catalog;
    private boolean caseSensitive;

    private static final List<DataTypeRoot> STRING_TYPES =
            Arrays.asList(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    private static final List<DataTypeRoot> BINARY_TYPES =
            Arrays.asList(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    private static final List<DataTypeRoot> INTEGER_TYPES =
            Arrays.asList(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT);
    private static final List<DataTypeRoot> FLOATING_POINT_TYPES =
            Arrays.asList(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE);

    private static final List<DataTypeRoot> DECIMAL_TYPES = Arrays.asList(DataTypeRoot.DECIMAL);

    private static final List<DataTypeRoot> TIMESTAMP_TYPES =
            Arrays.asList(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);

    protected UpdatedDataFieldsProcessFunctionBase(
            CatalogLoader catalogLoader, TypeMapping typeMapping) {
        this.catalogLoader = catalogLoader;
        this.typeMapping = typeMapping;
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.18-.
     */
    public void open(OpenContext openContext) throws Exception {
        open(new Configuration());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 2.0+.
     */
    public void open(Configuration parameters) {
        this.catalog = catalogLoader.load();
        this.caseSensitive = this.catalog.caseSensitive();
    }

    protected void applySchemaChange(
            SchemaManager schemaManager,
            SchemaChange schemaChange,
            Identifier identifier,
            CdcSchema newSchema)
            throws Exception {
        if (schemaChange instanceof SchemaChange.AddColumn) {
            try {
                catalog.alterTable(identifier, schemaChange, false);
            } catch (Catalog.ColumnAlreadyExistException e) {
                // This is normal. For example when a table is split into multiple database tables,
                // all these tables will be added the same column. However, schemaManager can't
                // handle duplicated column adds, so we just catch the exception and log it.
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to perform SchemaChange.AddColumn {}, "
                                    + "possibly due to duplicated column name",
                            schemaChange,
                            e);
                }
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnType) {
            SchemaChange.UpdateColumnType updateColumnType =
                    (SchemaChange.UpdateColumnType) schemaChange;
            String topLevelFieldName = updateColumnType.fieldNames()[0];
            TableSchema oldSchema =
                    schemaManager.latestOrThrow("Table does not exist. This is unexpected.");
            DataType oldTopLevelFieldType =
                    new RowType(oldSchema.fields()).getField(topLevelFieldName).type();
            DataType newTopLevelFieldType =
                    new RowType(newSchema.fields()).getField(topLevelFieldName).type();

            // For complex types, extract the top level type to check type context (e.g.,
            // ARRAY<BIGINT> instead of just BIGINT)
            switch (canConvert(oldTopLevelFieldType, newTopLevelFieldType, typeMapping)) {
                case CONVERT:
                    catalog.alterTable(identifier, schemaChange, false);
                    break;
                case EXCEPTION:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert field %s from type %s to %s of Paimon table %s.",
                                    topLevelFieldName,
                                    oldTopLevelFieldType,
                                    newTopLevelFieldType,
                                    identifier.getFullName()));
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnComment) {
            catalog.alterTable(identifier, schemaChange, false);
        } else if (schemaChange instanceof SchemaChange.UpdateComment) {
            catalog.alterTable(identifier, schemaChange, false);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported schema change class "
                            + schemaChange.getClass().getName()
                            + ", content "
                            + schemaChange);
        }
    }

    public static ConvertAction canConvert(
            DataType oldType, DataType newType, TypeMapping typeMapping) {
        if (oldType.equalsIgnoreNullable(newType)) {
            if (oldType.isNullable() && !newType.isNullable()) {
                return ConvertAction.EXCEPTION; // Cannot make nullable field non-nullable
            }
            return ConvertAction.CONVERT;
        }

        if (oldType.getTypeRoot() == DataTypeRoot.ARRAY
                && newType.getTypeRoot() == DataTypeRoot.ARRAY) {

            ArrayType oldArrayType = (ArrayType) oldType;
            ArrayType newArrayType = (ArrayType) newType;
            return canConvertArray(oldArrayType, newArrayType, typeMapping);
        }

        if (oldType.getTypeRoot() == DataTypeRoot.MAP
                && newType.getTypeRoot() == DataTypeRoot.MAP) {
            MapType oldMapType = (MapType) oldType;
            MapType newMapType = (MapType) newType;

            return canConvertMap(oldMapType, newMapType, typeMapping);
        }

        if (oldType.getTypeRoot() == DataTypeRoot.MULTISET
                && newType.getTypeRoot() == DataTypeRoot.MULTISET) {
            MultisetType oldMultisetType = (MultisetType) oldType;
            MultisetType newMultisetType = (MultisetType) newType;

            return canConvertMultisetType(oldMultisetType, newMultisetType, typeMapping);
        }

        if (oldType.getTypeRoot() == DataTypeRoot.ROW
                && newType.getTypeRoot() == DataTypeRoot.ROW) {
            return canConvertRowType((RowType) oldType, (RowType) newType, typeMapping);
        }

        int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
        int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        // object can always be converted to string
        if ((oldIdx < 0 && newIdx >= 0)
                && typeMapping.containsMode(
                        TypeMapping.TypeMappingMode.ALLOW_NON_STRING_TO_STRING)) {
            return ConvertAction.CONVERT;
        }

        oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = DECIMAL_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = DECIMAL_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(newType) <= DataTypeChecks.getPrecision(oldType)
                            && DataTypeChecks.getScale(newType) <= DataTypeChecks.getScale(oldType)
                    ? ConvertAction.IGNORE
                    : ConvertAction.CONVERT;
        }

        oldIdx = TIMESTAMP_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = TIMESTAMP_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getPrecision(oldType) <= DataTypeChecks.getPrecision(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        return ConvertAction.EXCEPTION;
    }

    private static ConvertAction canConvertArray(
            ArrayType oldArrayType, ArrayType newArrayType, TypeMapping typeMapping) {
        if (oldArrayType.isNullable() && !newArrayType.isNullable()) {
            return ConvertAction.EXCEPTION;
        }

        return canConvert(
                oldArrayType.getElementType(), newArrayType.getElementType(), typeMapping);
    }

    private static ConvertAction canConvertMap(
            MapType oldMapType, MapType newMapType, TypeMapping typeMapping) {
        if (oldMapType.isNullable() && !newMapType.isNullable()) {
            return ConvertAction.EXCEPTION;
        }

        // For map keys, don't allow key type evolution
        // hashcode will be different even if the value of the key is the same
        if (!oldMapType.getKeyType().equals(newMapType.getKeyType())) {
            return ConvertAction.EXCEPTION;
        }

        return canConvert(oldMapType.getValueType(), newMapType.getValueType(), typeMapping);
    }

    private static ConvertAction canConvertRowType(
            RowType oldRowType, RowType newRowType, TypeMapping typeMapping) {
        Map<String, DataField> oldFieldMap = new HashMap<>();
        for (DataField field : oldRowType.getFields()) {
            oldFieldMap.put(field.name(), field);
        }

        Map<String, DataField> newFieldMap = new HashMap<>();
        for (DataField field : newRowType.getFields()) {
            newFieldMap.put(field.name(), field);
        }

        // Rule 1: Check all non-nullable fields in the old type must exist in the new type
        for (DataField oldField : oldRowType.getFields()) {
            if (!oldField.type().isNullable()) {
                if (!newFieldMap.containsKey(oldField.name())) {
                    return ConvertAction.EXCEPTION;
                }
            }
        }

        // Rule 2: All fields common to both schemas must have compatible types
        boolean needsConversion = false;
        for (DataField newField : newRowType.getFields()) {
            DataField oldField = oldFieldMap.get(newField.name());
            if (oldField != null) {
                ConvertAction fieldAction =
                        canConvert(oldField.type(), newField.type(), typeMapping);
                if (fieldAction == ConvertAction.EXCEPTION) {
                    return ConvertAction.EXCEPTION;
                }
                if (fieldAction == ConvertAction.CONVERT) {
                    needsConversion = true;
                }
            } else {
                // Rule 3: New fields must be nullable
                if (!newField.type().isNullable()) {
                    return ConvertAction.EXCEPTION;
                }
                needsConversion = true;
            }
        }

        return needsConversion ? ConvertAction.CONVERT : ConvertAction.IGNORE;
    }

    private static ConvertAction canConvertMultisetType(
            MultisetType oldMultisetType, MultisetType newMultisetType, TypeMapping typeMapping) {

        if (oldMultisetType.isNullable() && !newMultisetType.isNullable()) {
            return ConvertAction.EXCEPTION;
        }

        return canConvert(
                oldMultisetType.getElementType(), newMultisetType.getElementType(), typeMapping);
    }

    protected List<SchemaChange> extractSchemaChanges(
            SchemaManager schemaManager, CdcSchema updatedSchema) {
        TableSchema oldTableSchema = schemaManager.latest().get();
        RowType oldRowType = oldTableSchema.logicalRowType();
        Map<String, DataField> oldFields = new HashMap<>();
        for (DataField oldField : oldRowType.getFields()) {
            oldFields.put(oldField.name(), oldField);
        }

        boolean allowDecimalTypeChange =
                this.typeMapping == null
                        || !this.typeMapping.containsMode(
                                TypeMapping.TypeMappingMode.DECIMAL_NO_CHANGE);

        List<SchemaChange> result = new ArrayList<>();
        for (DataField newField : updatedSchema.fields()) {
            String newFieldName = StringUtils.toLowerCaseIfNeed(newField.name(), caseSensitive);
            if (oldFields.containsKey(newFieldName)) {
                DataField oldField = oldFields.get(newFieldName);
                // 1. we compare by ignoring nullable, because partition keys and primary keys might
                // be nullable in source database, but they can't be null in Paimon
                // 2. we compare by ignoring field id, the field ID is newly created and may be
                // different, we should ignore it
                if (oldField.type().copy(true).equalsIgnoreFieldId(newField.type().copy(true))) {
                    // update column comment
                    if (newField.description() != null
                            && !newField.description().equals(oldField.description())) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[] {newFieldName}, newField.description()));
                    }
                } else {
                    if (oldField.type().is(DataTypeRoot.DECIMAL) && !allowDecimalTypeChange) {
                        continue;
                    }
                    // Generate nested column updates if needed
                    NestedSchemaUtils.generateNestedColumnUpdates(
                            Collections.singletonList(newFieldName),
                            oldField.type(),
                            newField.type(),
                            result);
                    // update column comment
                    if (newField.description() != null) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[] {newFieldName}, newField.description()));
                    }
                }
            } else {
                // add column
                result.add(
                        SchemaChange.addColumn(
                                newFieldName, newField.type(), newField.description(), null));
            }
        }

        if (updatedSchema.comment() != null
                && !updatedSchema.comment().equals(oldTableSchema.comment())) {
            // update table comment
            result.add(SchemaChange.updateComment(updatedSchema.comment()));
        }
        return result;
    }

    protected List<DataField> actualUpdatedDataFields(
            List<DataField> newFields, Set<FieldIdentifier> latestFields) {
        return newFields.stream()
                .filter(dataField -> !latestFields.contains(new FieldIdentifier(dataField)))
                .collect(Collectors.toList());
    }

    protected Set<FieldIdentifier> updateLatestFields(SchemaManager schemaManager) {
        RowType oldRowType = schemaManager.latest().get().logicalRowType();
        return oldRowType.getFields().stream()
                .map(FieldIdentifier::new)
                .collect(Collectors.toSet());
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    /**
     * Return type of {@link UpdatedDataFieldsProcessFunction#canConvert}. This enum indicates the
     * action to perform.
     */
    public enum ConvertAction {

        /** {@code oldType} can be converted to {@code newType}. */
        CONVERT,

        /**
         * {@code oldType} and {@code newType} belongs to the same type family, but old type has
         * higher precision than new type. Ignore this convert request.
         */
        IGNORE,

        /**
         * {@code oldType} and {@code newType} belongs to different type family. Throw an exception
         * indicating that this convert request cannot be handled.
         */
        EXCEPTION
    }
}
