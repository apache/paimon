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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VecColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.CastedArrayColumnVector;
import org.apache.paimon.data.columnar.heap.CastedMapColumnVector;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.heap.CastedVectorColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.shredding.ShreddingBatchAssembler;
import org.apache.paimon.data.shredding.ShreddingReadPlan;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.PaimonShreddingUtils.FieldToExtract;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.data.variant.VariantPathSegment;
import org.apache.paimon.format.parquet.reader.ParquetReaderUtil;
import org.apache.paimon.format.shredding.ShreddingReadPlanFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.data.variant.Variant.METADATA;
import static org.apache.paimon.data.variant.Variant.VALUE;
import static org.apache.paimon.data.variant.VariantMetadataUtils.path;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;

/**
 * Parquet read plan factory for Variant physical layouts.
 *
 * <p>The plan reads the physical Variant row layout from Parquet, then materializes it back to the
 * logical Variant or Variant projection row in the shared shredding batch assembly path.
 */
public class VariantShreddingReadPlanFactory implements ShreddingReadPlanFactory {

    private final RowType logicalRowType;
    private final boolean caseSensitive;

    public VariantShreddingReadPlanFactory(RowType logicalRowType, boolean caseSensitive) {
        this.logicalRowType = logicalRowType;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public RowType logicalRowType() {
        return logicalRowType;
    }

    @Override
    public boolean shouldCreateReadPlan(
            Map<String, Map<String, String>> fieldMetadata, @Nullable Object fileSchema) {
        return fileSchema instanceof GroupType && containsVariantFields(logicalRowType);
    }

    @Override
    public ShreddingReadPlan createReadPlan(
            Map<String, Map<String, String>> fieldMetadata, @Nullable Object fileSchema) {
        return new VariantShreddingReadPlan(logicalRowType, (GroupType) fileSchema, caseSensitive);
    }

    /** Extracts the physical Variant file schema from a Parquet field. */
    public static RowType variantFileType(Type fileType) {
        boolean isShredded =
                fileType.asGroupType().containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME);
        if (isShredded) {
            return (RowType) ParquetSchemaConverter.convertToPaimonField(fileType).type();
        }

        List<DataField> dataFields = new ArrayList<>();
        dataFields.add(new DataField(0, VALUE, DataTypes.BYTES().notNull()));
        dataFields.add(new DataField(1, METADATA, DataTypes.BYTES().notNull()));
        return new RowType(dataFields);
    }

    /** Clips a Variant Parquet field according to the logical Variant read type. */
    @Nullable
    public static Type clipParquetType(DataType logicalType, Type parquetType) {
        if (logicalType instanceof VariantType) {
            return parquetType;
        }
        if (VariantMetadataUtils.isVariantRowType(logicalType)) {
            return clipVariantType((RowType) logicalType, parquetType.asGroupType());
        }
        return null;
    }

    /** Clips a Variant Parquet field according to the logical Variant row read type. */
    public static Type clipVariantType(RowType variantRowType, GroupType parquetType) {
        if (!parquetType.containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)) {
            return parquetType;
        }

        boolean canClip = true;
        Set<String> fieldsToRead = new HashSet<>();
        for (DataField field : variantRowType.getFields()) {
            String path = path(field.description());
            VariantPathSegment[] pathSegments = VariantPathSegment.parse(path);
            if (pathSegments.length < 1) {
                canClip = false;
                break;
            }

            // TODO: support nested column pruning.
            VariantPathSegment pathSegment = pathSegments[0];
            if (pathSegment instanceof VariantPathSegment.ObjectExtraction) {
                fieldsToRead.add(((VariantPathSegment.ObjectExtraction) pathSegment).getKey());
            } else {
                canClip = false;
                break;
            }
        }

        if (!canClip) {
            return parquetType;
        }

        List<Type> typedFieldsToRead = new ArrayList<>();
        GroupType typedValue =
                parquetType.getType(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME).asGroupType();
        for (Type field : typedValue.getFields()) {
            if (fieldsToRead.contains(field.getName())) {
                typedFieldsToRead.add(field);
                fieldsToRead.remove(field.getName());
            }
        }

        List<Type> rowGroupFields = new ArrayList<>();
        rowGroupFields.add(parquetType.getType(PaimonShreddingUtils.METADATA_FIELD_NAME));
        if (!fieldsToRead.isEmpty()) {
            rowGroupFields.add(parquetType.getType(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
        }
        if (!typedFieldsToRead.isEmpty()) {
            rowGroupFields.add(typedValue.withNewFields(typedFieldsToRead));
        }
        return parquetType.withNewFields(rowGroupFields);
    }

    private static boolean containsVariantFields(DataType dataType) {
        if (dataType instanceof VariantType || VariantMetadataUtils.isVariantRowType(dataType)) {
            return true;
        }
        if (dataType instanceof RowType) {
            for (DataField field : ((RowType) dataType).getFields()) {
                if (containsVariantFields(field.type())) {
                    return true;
                }
            }
        }
        if (dataType instanceof ArrayType) {
            return containsVariantFields(((ArrayType) dataType).getElementType());
        }
        if (dataType instanceof VectorType) {
            return containsVariantFields(((VectorType) dataType).getElementType());
        }
        if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return containsVariantFields(mapType.getKeyType())
                    || containsVariantFields(mapType.getValueType());
        }
        return false;
    }

    private static RowType buildPhysicalRowType(
            RowType logicalRowType, GroupType parquetType, boolean caseSensitive) {
        List<DataField> fields = new ArrayList<>();
        for (DataField field : logicalRowType.getFields()) {
            Type fileType = matchParquetField(parquetType, field.name(), caseSensitive);
            fields.add(field.newType(buildPhysicalType(field.type(), fileType, caseSensitive)));
        }
        return new RowType(logicalRowType.isNullable(), fields);
    }

    private static DataType buildPhysicalType(
            DataType logicalType, Type fileType, boolean caseSensitive) {
        if (logicalType instanceof VariantType
                || VariantMetadataUtils.isVariantRowType(logicalType)) {
            Type clippedType =
                    logicalType instanceof RowType
                            ? clipVariantType((RowType) logicalType, fileType.asGroupType())
                            : fileType;
            return variantFileType(clippedType).copy(logicalType.isNullable());
        }

        switch (logicalType.getTypeRoot()) {
            case ROW:
                RowType rowType = (RowType) logicalType;
                GroupType rowGroup = fileType.asGroupType();
                List<DataField> fields = new ArrayList<>();
                for (DataField field : rowType.getFields()) {
                    fields.add(
                            field.newType(
                                    buildPhysicalType(
                                            field.type(),
                                            matchParquetField(
                                                    rowGroup, field.name(), caseSensitive),
                                            caseSensitive)));
                }
                return new RowType(rowType.isNullable(), fields);
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                return new ArrayType(
                        arrayType.isNullable(),
                        buildPhysicalType(
                                arrayType.getElementType(),
                                parquetListElementType(fileType.asGroupType()),
                                caseSensitive));
            case VECTOR:
                VectorType vectorType = (VectorType) logicalType;
                return new VectorType(
                        vectorType.isNullable(),
                        vectorType.getLength(),
                        buildPhysicalType(
                                vectorType.getElementType(),
                                parquetListElementType(fileType.asGroupType()),
                                caseSensitive));
            case MAP:
                MapType mapType = (MapType) logicalType;
                Pair<Type, Type> keyValueType = parquetMapKeyValueType(fileType.asGroupType());
                return new MapType(
                        mapType.isNullable(),
                        buildPhysicalType(
                                mapType.getKeyType(), keyValueType.getKey(), caseSensitive),
                        buildPhysicalType(
                                mapType.getValueType(), keyValueType.getValue(), caseSensitive));
            default:
                return logicalType;
        }
    }

    private static Type matchParquetField(
            GroupType group, String fieldName, boolean caseSensitive) {
        if (caseSensitive) {
            return group.getType(fieldName);
        }
        Type matched = null;
        for (Type field : group.getFields()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                if (matched != null) {
                    throw new RuntimeException(
                            String.format(
                                    "Found duplicate field(s) \"%s\": [%s, %s] in case-insensitive mode",
                                    fieldName, matched.getName(), field.getName()));
                }
                matched = field;
            }
        }
        return matched == null ? group.getType(fieldName) : matched;
    }

    private static class VariantShreddingReadPlan implements ShreddingReadPlan {

        private final RowType logicalRowType;
        private final RowType physicalRowType;

        private VariantShreddingReadPlan(
                RowType logicalRowType, GroupType fileSchema, boolean caseSensitive) {
            this.logicalRowType = logicalRowType;
            this.physicalRowType = buildPhysicalRowType(logicalRowType, fileSchema, caseSensitive);
        }

        @Override
        public RowType logicalRowType() {
            return logicalRowType;
        }

        @Override
        public RowType physicalRowType() {
            return physicalRowType;
        }

        @Override
        public ShreddingBatchAssembler batchAssembler() {
            return new VariantShreddingBatchAssembler(logicalRowType, physicalRowType);
        }
    }

    private static class VariantShreddingBatchAssembler implements ShreddingBatchAssembler {

        private final RowType logicalRowType;
        private final RowType physicalRowType;

        private VariantShreddingBatchAssembler(RowType logicalRowType, RowType physicalRowType) {
            this.logicalRowType = logicalRowType;
            this.physicalRowType = physicalRowType;
        }

        @Override
        public VectorizedColumnBatch assemble(VectorizedColumnBatch physicalBatch) {
            ColumnVector[] vectors = new ColumnVector[logicalRowType.getFieldCount()];
            for (int i = 0; i < vectors.length; i++) {
                vectors[i] =
                        assembleVector(
                                logicalRowType.getTypeAt(i),
                                physicalRowType.getTypeAt(i),
                                physicalBatch.columns[i]);
            }
            return physicalBatch.copy(vectors);
        }
    }

    private static ColumnVector assembleVector(
            DataType logicalType, DataType physicalType, ColumnVector physicalVector) {
        if (logicalType instanceof VariantType
                || VariantMetadataUtils.isVariantRowType(logicalType)) {
            return assembleVariantVector(logicalType, physicalType, physicalVector);
        }

        switch (logicalType.getTypeRoot()) {
            case ROW:
                return assembleRowVector(
                        (RowType) logicalType, (RowType) physicalType, physicalVector);
            case ARRAY:
                return assembleArrayVector(
                        (ArrayType) logicalType, (ArrayType) physicalType, physicalVector);
            case VECTOR:
                return assembleVectorVector(
                        (VectorType) logicalType, (VectorType) physicalType, physicalVector);
            case MAP:
                return assembleMapVector(
                        (MapType) logicalType, (MapType) physicalType, physicalVector);
            default:
                return physicalVector;
        }
    }

    private static ColumnVector assembleVariantVector(
            DataType logicalType, DataType physicalType, ColumnVector physicalVector) {
        Preconditions.checkArgument(
                physicalVector instanceof CastedRowColumnVector,
                "Variant physical vector should be a row vector.");
        CastedRowColumnVector input = (CastedRowColumnVector) physicalVector;
        WritableColumnVector output =
                ParquetReaderUtil.createWritableColumnVector(
                        input.getElementsAppended(), logicalType);
        org.apache.paimon.data.variant.VariantSchema variantSchema =
                PaimonShreddingUtils.buildVariantSchema((RowType) physicalType);
        FieldToExtract[] fieldsToExtract =
                PaimonShreddingUtils.getFieldsToExtract(logicalType, variantSchema);
        if (fieldsToExtract == null) {
            PaimonShreddingUtils.assembleVariantBatch(input, output, variantSchema);
        } else {
            PaimonShreddingUtils.assembleVariantStructBatch(
                    input, output, variantSchema, fieldsToExtract, logicalType);
        }
        return ParquetReaderUtil.createReadableColumnVector(logicalType, output);
    }

    private static ColumnVector assembleRowVector(
            RowType logicalType, RowType physicalType, ColumnVector physicalVector) {
        ColumnVector[] physicalChildren = physicalVector.getChildren();
        ColumnVector[] logicalChildren = new ColumnVector[logicalType.getFieldCount()];
        boolean changed = false;
        for (int i = 0; i < logicalChildren.length; i++) {
            logicalChildren[i] =
                    assembleVector(
                            logicalType.getTypeAt(i),
                            physicalType.getTypeAt(i),
                            physicalChildren[i]);
            changed |= logicalChildren[i] != physicalChildren[i];
        }
        if (!changed) {
            return physicalVector;
        }
        Preconditions.checkArgument(
                physicalVector instanceof CastedRowColumnVector,
                "Nested Variant row parent should be a casted row vector.");
        return ((CastedRowColumnVector) physicalVector).copy(logicalChildren);
    }

    private static ColumnVector assembleArrayVector(
            ArrayType logicalType, ArrayType physicalType, ColumnVector physicalVector) {
        ArrayColumnVector arrayVector = (ArrayColumnVector) physicalVector;
        ColumnVector physicalChild = arrayVector.getColumnVector();
        ColumnVector logicalChild =
                assembleVector(
                        logicalType.getElementType(), physicalType.getElementType(), physicalChild);
        if (logicalChild == physicalChild) {
            return physicalVector;
        }
        Preconditions.checkArgument(
                physicalVector instanceof CastedArrayColumnVector,
                "Nested Variant array parent should be a casted array vector.");
        return ((CastedArrayColumnVector) physicalVector).copy(logicalChild);
    }

    private static ColumnVector assembleVectorVector(
            VectorType logicalType, VectorType physicalType, ColumnVector physicalVector) {
        VecColumnVector vecColumnVector = (VecColumnVector) physicalVector;
        ColumnVector physicalChild = vecColumnVector.getColumnVector();
        ColumnVector logicalChild =
                assembleVector(
                        logicalType.getElementType(), physicalType.getElementType(), physicalChild);
        if (logicalChild == physicalChild) {
            return physicalVector;
        }
        Preconditions.checkArgument(
                physicalVector instanceof CastedVectorColumnVector,
                "Nested Variant vector parent should be a casted vector vector.");
        return ((CastedVectorColumnVector) physicalVector).copy(logicalChild);
    }

    private static ColumnVector assembleMapVector(
            MapType logicalType, MapType physicalType, ColumnVector physicalVector) {
        ColumnVector[] physicalChildren = physicalVector.getChildren();
        ColumnVector[] logicalChildren = new ColumnVector[physicalChildren.length];
        logicalChildren[0] =
                assembleVector(
                        logicalType.getKeyType(), physicalType.getKeyType(), physicalChildren[0]);
        logicalChildren[1] =
                assembleVector(
                        logicalType.getValueType(),
                        physicalType.getValueType(),
                        physicalChildren[1]);
        if (logicalChildren[0] == physicalChildren[0]
                && logicalChildren[1] == physicalChildren[1]) {
            return physicalVector;
        }
        Preconditions.checkArgument(
                physicalVector instanceof CastedMapColumnVector,
                "Nested Variant map parent should be a casted map vector.");
        return ((CastedMapColumnVector) physicalVector).copy(logicalChildren);
    }
}
