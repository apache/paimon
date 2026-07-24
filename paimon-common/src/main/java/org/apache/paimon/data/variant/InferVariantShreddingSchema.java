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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Infer a schema when there are Variant values in the shredding schema. Only VariantType values at
 * the top level or nested in row fields are shredded. VariantType nested in arrays or maps are not
 * shredded.
 */
public class InferVariantShreddingSchema {

    private final RowType schema;
    private final List<List<Integer>> pathsToVariant;
    private final int maxSchemaWidth;
    private final int maxSchemaDepth;
    private final double minFieldCardinalityRatio;

    public InferVariantShreddingSchema(
            RowType schema,
            int maxSchemaWidth,
            int maxSchemaDepth,
            double minFieldCardinalityRatio) {
        this.schema = schema;
        this.pathsToVariant = getPathsToVariant(schema);
        this.maxSchemaWidth = maxSchemaWidth;
        this.maxSchemaDepth = maxSchemaDepth;
        this.minFieldCardinalityRatio = minFieldCardinalityRatio;
    }

    /** Infer schema from a list of rows. */
    public RowType inferSchema(List<InternalRow> rows) {
        return inferInitial(rows, Integer.MAX_VALUE).physicalRowType();
    }

    AdaptiveInferenceResult inferInitial(List<InternalRow> rows, int effectiveSampleSize) {
        InferenceEvidence evidence = analyze(rows);
        MaxFields maxFields = new MaxFields(maxSchemaWidth);
        Map<List<Integer>, RowType> inferredSchemas = new HashMap<>();
        Map<List<Integer>, DataType> selectedSchemas = new HashMap<>();

        for (List<Integer> path : pathsToVariant) {
            ColumnEvidence column = evidence.columns.get(path);
            double numNonNullValues = column == null ? 0 : column.rootValueCount;
            DataType simpleSchema = column == null ? null : column.observedSchema;

            // Don't infer a schema for fields that appear in less than minFieldCardinalityRatio
            int minCardinality = (int) Math.ceil(numNonNullValues * minFieldCardinalityRatio);

            DataType finalizedSchema =
                    finalizeSimpleSchema(simpleSchema, minCardinality, maxFields);
            selectedSchemas.put(path, finalizedSchema);
            RowType shreddingSchema = PaimonShreddingUtils.variantShreddingSchema(finalizedSchema);
            inferredSchemas.put(path, shreddingSchema);
        }

        // Insert each inferred schema into the full schema
        return new AdaptiveInferenceResult(
                updateSchema(schema, inferredSchemas, new ArrayList<>()),
                limitEvidence(evidence, effectiveSampleSize),
                selectedSchemas);
    }

    InferenceEvidence analyze(List<InternalRow> rows) {
        Map<List<Integer>, ColumnEvidence> columns = new HashMap<>();
        for (List<Integer> path : pathsToVariant) {
            double rootValueCount = 0;
            DataType observedSchema = null;
            for (InternalRow row : rows) {
                Variant variant = getValueAtPath(schema, row, path);
                if (variant != null) {
                    rootValueCount++;
                    observedSchema =
                            mergeSchema(
                                    observedSchema,
                                    schemaOf((GenericVariant) variant, maxSchemaDepth));
                }
            }
            columns.put(path, new ColumnEvidence(rootValueCount, observedSchema));
        }
        return new InferenceEvidence(columns);
    }

    AdaptiveInferenceResult inferAdaptive(
            InferenceEvidence previousEvidence,
            Map<List<Integer>, DataType> previousSelectedSchemas,
            List<InternalRow> rows,
            int effectiveSampleSize,
            double admissionRatio,
            double retentionRatio) {
        InferenceEvidence currentEvidence = analyze(rows);
        InferenceEvidence combinedEvidence =
                combineEvidence(previousEvidence, currentEvidence, effectiveSampleSize);
        MaxFields maxFields = new MaxFields(maxSchemaWidth);
        Map<List<Integer>, RowType> shreddingSchemas = new HashMap<>();
        Map<List<Integer>, DataType> selectedSchemas = new HashMap<>();

        for (List<Integer> path : pathsToVariant) {
            ColumnEvidence combined = combinedEvidence.columns.get(path);
            ColumnEvidence current = currentEvidence.columns.get(path);
            DataType previousSelected = previousSelectedSchemas.get(path);
            DataType selected =
                    finalizeAdaptiveSchema(
                            combined == null ? null : combined.observedSchema,
                            current == null ? null : current.observedSchema,
                            previousSelected,
                            combined == null ? 0 : combined.rootValueCount,
                            admissionRatio,
                            retentionRatio,
                            maxFields);
            selectedSchemas.put(path, selected);
            shreddingSchemas.put(path, PaimonShreddingUtils.variantShreddingSchema(selected));
        }

        return new AdaptiveInferenceResult(
                updateSchema(schema, shreddingSchemas, new ArrayList<>()),
                combinedEvidence,
                selectedSchemas);
    }

    private InferenceEvidence combineEvidence(
            InferenceEvidence previous, InferenceEvidence current, int effectiveSampleSize) {
        Map<List<Integer>, ColumnEvidence> columns = new HashMap<>();
        for (List<Integer> path : pathsToVariant) {
            ColumnEvidence previousColumn = previous == null ? null : previous.columns.get(path);
            ColumnEvidence currentColumn = current.columns.get(path);
            if (currentColumn == null || currentColumn.rootValueCount == 0) {
                if (previousColumn != null) {
                    columns.put(path, previousColumn);
                } else {
                    columns.put(path, new ColumnEvidence(0, null));
                }
                continue;
            }

            ColumnEvidence boundedPrevious = scaleToAtMost(previousColumn, effectiveSampleSize);
            double rootValueCount =
                    currentColumn.rootValueCount
                            + (boundedPrevious == null ? 0 : boundedPrevious.rootValueCount);
            DataType observedSchema =
                    mergeSchema(
                            boundedPrevious == null ? null : boundedPrevious.observedSchema,
                            currentColumn.observedSchema);
            columns.put(
                    path,
                    scaleToAtMost(
                            new ColumnEvidence(rootValueCount, observedSchema),
                            effectiveSampleSize));
        }
        return new InferenceEvidence(columns);
    }

    private InferenceEvidence limitEvidence(InferenceEvidence evidence, int effectiveSampleSize) {
        Map<List<Integer>, ColumnEvidence> columns = new HashMap<>();
        for (Map.Entry<List<Integer>, ColumnEvidence> entry : evidence.columns.entrySet()) {
            columns.put(entry.getKey(), scaleToAtMost(entry.getValue(), effectiveSampleSize));
        }
        return new InferenceEvidence(columns);
    }

    private ColumnEvidence scaleToAtMost(ColumnEvidence evidence, int maxRootValueCount) {
        if (evidence == null
                || evidence.rootValueCount == 0
                || evidence.rootValueCount <= maxRootValueCount) {
            return evidence;
        }
        double scale = maxRootValueCount / evidence.rootValueCount;
        return new ColumnEvidence(
                maxRootValueCount, scaleFieldCounts(evidence.observedSchema, scale));
    }

    private DataType scaleFieldCounts(DataType dataType, double scale) {
        if (dataType instanceof RowType) {
            List<DataField> fields = new ArrayList<>();
            for (DataField field : ((RowType) dataType).getFields()) {
                fields.add(
                        new DataField(
                                field.id(),
                                field.name(),
                                scaleFieldCounts(field.type(), scale),
                                String.valueOf(getFieldCount(field) * scale)));
            }
            return new RowType(dataType.isNullable(), fields);
        }
        if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            return new ArrayType(
                    arrayType.isNullable(), scaleFieldCounts(arrayType.getElementType(), scale));
        }
        return dataType;
    }

    /**
     * Create a list of paths to Variant values in the schema. Variant fields nested in arrays or
     * maps are not included. For example, if the schema is {@code row<v: variant, row<a: int, b:
     * int, c: variant>>} the function will return [[0], [1, 2]]
     */
    private List<List<Integer>> getPathsToVariant(RowType schema) {
        List<List<Integer>> result = new ArrayList<>();
        List<DataField> fields = schema.getFields();

        for (int idx = 0; idx < fields.size(); idx++) {
            DataField field = fields.get(idx);
            DataType dataType = field.type();

            if (dataType instanceof VariantType) {
                List<Integer> path = new ArrayList<>();
                path.add(idx);
                result.add(path);
            } else if (dataType instanceof RowType) {
                // Prepend this index to each downstream path
                List<List<Integer>> innerPaths = getPathsToVariant((RowType) dataType);
                for (List<Integer> path : innerPaths) {
                    List<Integer> fullPath = new ArrayList<>();
                    fullPath.add(idx);
                    fullPath.addAll(path);
                    result.add(fullPath);
                }
            }
        }

        return result;
    }

    /**
     * Return the Variant at the given path in the schema, or null if the Variant value or any of
     * its containing rows is null.
     */
    private Variant getValueAtPath(RowType schema, InternalRow row, List<Integer> path) {
        return getValueAtPathHelper(schema, row, path, 0);
    }

    private Variant getValueAtPathHelper(
            RowType schema, InternalRow row, List<Integer> path, int pathIndex) {
        int idx = path.get(pathIndex);

        if (row.isNullAt(idx)) {
            return null;
        } else if (pathIndex == path.size() - 1) {
            // We've reached the Variant value
            return row.getVariant(idx);
        } else {
            // The field must be a row
            RowType childRowType = (RowType) schema.getFields().get(idx).type();
            InternalRow childRow = row.getRow(idx, childRowType.getFieldCount());
            return getValueAtPathHelper(childRowType, childRow, path, pathIndex + 1);
        }
    }

    /**
     * Return an appropriate schema for shredding a Variant value. It is similar to the
     * SchemaOfVariant expression, but the rules are somewhat different, because we want the types
     * to be consistent with what will be allowed during shredding. E.g. SchemaOfVariant will
     * consider the common type across Integer and Double to be double, but we consider it to be
     * VariantType, since shredding will not allow those types to be written to the same
     * typed_value. We also maintain metadata on row fields to track how frequently they occur. Rare
     * fields are dropped in the final schema.
     */
    private DataType schemaOf(GenericVariant v, int maxDepth) {
        GenericVariantUtil.Type type = v.getType();

        switch (type) {
            case OBJECT:
                if (maxDepth <= 0) {
                    return DataTypes.VARIANT();
                }

                int size = v.objectSize();
                List<DataField> fields = new ArrayList<>(size);

                for (int i = 0; i < size; i++) {
                    GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                    DataType fieldType = schemaOf(field.value, maxDepth - 1);
                    // Store count in description temporarily (will be used in mergeRowTypes)
                    DataField dataField = new DataField(i, field.key, fieldType, "1");
                    fields.add(dataField);
                }

                // According to the variant spec, object fields must be sorted alphabetically
                for (int i = 1; i < size; i++) {
                    if (fields.get(i - 1).name().compareTo(fields.get(i).name()) >= 0) {
                        throw new RuntimeException(
                                "Variant object fields must be sorted alphabetically");
                    }
                }

                return new RowType(fields);

            case ARRAY:
                if (maxDepth <= 0) {
                    return DataTypes.VARIANT();
                }

                DataType elementType = null;
                for (int i = 0; i < v.arraySize(); i++) {
                    elementType =
                            mergeSchema(
                                    elementType, schemaOf(v.getElementAtIndex(i), maxDepth - 1));
                }
                return new ArrayType(elementType == null ? DataTypes.VARIANT() : elementType);

            case NULL:
                return null;

            case BOOLEAN:
                return DataTypes.BOOLEAN();

            case LONG:
                // Compute the smallest decimal that can contain this value
                BigDecimal d = BigDecimal.valueOf(v.getLong());
                int precision = d.precision();
                if (precision <= 18) {
                    return DataTypes.DECIMAL(precision, 0);
                } else {
                    return DataTypes.BIGINT();
                }

            case STRING:
                return DataTypes.STRING();

            case DOUBLE:
                return DataTypes.DOUBLE();

            case DECIMAL:
                BigDecimal dec = v.getDecimal();
                int decPrecision = dec.precision();
                int decScale = dec.scale();
                // Ensure precision is at least scale + 1 to be valid
                if (decPrecision < decScale) {
                    decPrecision = decScale;
                }
                // Ensure precision is at least 1
                if (decPrecision == 0) {
                    decPrecision = 1;
                }
                return DataTypes.DECIMAL(decPrecision, decScale);

            case DATE:
                return DataTypes.DATE();

            case TIMESTAMP:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();

            case TIMESTAMP_NTZ:
                return DataTypes.TIMESTAMP();

            case FLOAT:
                return DataTypes.FLOAT();

            case BINARY:
                return DataTypes.BYTES();

            default:
                return DataTypes.VARIANT();
        }
    }

    private double getFieldCount(DataField field) {
        // Read count from description field
        String desc = field.description();
        if (desc == null || desc.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Field '%s' is missing count in description. This should not happen during schema inference.",
                            field.name()));
        }
        return Double.parseDouble(desc);
    }

    /** Merge two decimals with possibly different scales. */
    private DataType mergeDecimal(DecimalType d1, DecimalType d2) {
        int scale = Math.max(d1.getScale(), d2.getScale());
        int range = Math.max(d1.getPrecision() - d1.getScale(), d2.getPrecision() - d2.getScale());

        if (range + scale > DecimalType.MAX_PRECISION) {
            // DecimalType can't support precision > 38
            return DataTypes.VARIANT();
        } else {
            return DataTypes.DECIMAL(range + scale, scale);
        }
    }

    private DataType mergeDecimalWithLong(DecimalType d) {
        if (d.getScale() == 0 && d.getPrecision() <= 18) {
            // It's an integer-like Decimal
            return DataTypes.BIGINT();
        } else {
            // Long can always fit in a Decimal(19, 0)
            return mergeDecimal(d, DataTypes.DECIMAL(19, 0));
        }
    }

    private DataType mergeSchema(DataType dt1, DataType dt2) {
        // Allow null to appear in any typed schema
        if (dt1 == null) {
            return dt2;
        } else if (dt2 == null) {
            return dt1;
        } else if (dt1 instanceof DecimalType && dt2 instanceof DecimalType) {
            return mergeDecimal((DecimalType) dt1, (DecimalType) dt2);
        } else if (dt1 instanceof DecimalType
                && dt2.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT) {
            return mergeDecimalWithLong((DecimalType) dt1);
        } else if (dt1.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT
                && dt2 instanceof DecimalType) {
            return mergeDecimalWithLong((DecimalType) dt2);
        } else if (dt1 instanceof RowType && dt2 instanceof RowType) {
            return mergeRowTypes((RowType) dt1, (RowType) dt2);
        } else if (dt1 instanceof ArrayType && dt2 instanceof ArrayType) {
            ArrayType a1 = (ArrayType) dt1;
            ArrayType a2 = (ArrayType) dt2;
            return new ArrayType(mergeSchema(a1.getElementType(), a2.getElementType()));
        } else if (dt1.equals(dt2)) {
            return dt1;
        } else {
            return DataTypes.VARIANT();
        }
    }

    private DataType mergeRowTypes(RowType s1, RowType s2) {
        List<DataField> fields1 = s1.getFields();
        List<DataField> fields2 = s2.getFields();
        List<DataField> newFields = new ArrayList<>();

        int f1Idx = 0;
        int f2Idx = 0;
        int maxRowFieldSize = 1000;
        int nextFieldId = 0;

        while (f1Idx < fields1.size()
                && f2Idx < fields2.size()
                && newFields.size() < maxRowFieldSize) {
            DataField field1 = fields1.get(f1Idx);
            DataField field2 = fields2.get(f2Idx);
            String f1Name = field1.name();
            String f2Name = field2.name();
            int comp = f1Name.compareTo(f2Name);

            if (comp == 0) {
                DataType dataType = mergeSchema(field1.type(), field2.type());
                double c1 = getFieldCount(field1);
                double c2 = getFieldCount(field2);
                // Store count in description
                DataField newField =
                        new DataField(nextFieldId++, f1Name, dataType, String.valueOf(c1 + c2));
                newFields.add(newField);
                f1Idx++;
                f2Idx++;
            } else if (comp < 0) {
                double count = getFieldCount(field1);
                DataField newField =
                        new DataField(
                                nextFieldId++, field1.name(), field1.type(), String.valueOf(count));
                newFields.add(newField);
                f1Idx++;
            } else {
                double count = getFieldCount(field2);
                DataField newField =
                        new DataField(
                                nextFieldId++, field2.name(), field2.type(), String.valueOf(count));
                newFields.add(newField);
                f2Idx++;
            }
        }

        while (f1Idx < fields1.size() && newFields.size() < maxRowFieldSize) {
            DataField field1 = fields1.get(f1Idx);
            double count = getFieldCount(field1);
            DataField newField =
                    new DataField(
                            nextFieldId++, field1.name(), field1.type(), String.valueOf(count));
            newFields.add(newField);
            f1Idx++;
        }

        while (f2Idx < fields2.size() && newFields.size() < maxRowFieldSize) {
            DataField field2 = fields2.get(f2Idx);
            double count = getFieldCount(field2);
            DataField newField =
                    new DataField(
                            nextFieldId++, field2.name(), field2.type(), String.valueOf(count));
            newFields.add(newField);
            f2Idx++;
        }

        return new RowType(newFields);
    }

    /** Return a new schema, with each VariantType replaced with its inferred shredding schema. */
    private RowType updateSchema(
            RowType schema, Map<List<Integer>, RowType> inferredSchemas, List<Integer> path) {

        List<DataField> fields = schema.getFields();
        List<DataField> newFields = new ArrayList<>(fields.size());

        for (int idx = 0; idx < fields.size(); idx++) {
            DataField field = fields.get(idx);
            DataType dataType = field.type();

            if (dataType instanceof VariantType) {
                List<Integer> fullPath = new ArrayList<>(path);
                fullPath.add(idx);
                if (!inferredSchemas.containsKey(fullPath)) {
                    throw new IllegalStateException(
                            String.format(
                                    "No inferred schema found for Variant field '%s' at path %s",
                                    field.name(), fullPath));
                }
                newFields.add(field.newType(inferredSchemas.get(fullPath)));
            } else if (dataType instanceof RowType) {
                List<Integer> fullPath = new ArrayList<>(path);
                fullPath.add(idx);
                RowType newType = updateSchema((RowType) dataType, inferredSchemas, fullPath);
                newFields.add(field.newType(newType));
            } else {
                newFields.add(field);
            }
        }

        return new RowType(newFields);
    }

    private DataType finalizeAdaptiveSchema(
            DataType combined,
            DataType current,
            DataType previousSelected,
            double rootValueCount,
            double admissionRatio,
            double retentionRatio,
            MaxFields maxFields) {
        maxFields.remaining--;
        if (maxFields.remaining <= 0) {
            return DataTypes.VARIANT();
        }

        if (current != null
                && previousSelected != null
                && !compatibleTypeFamilies(previousSelected, current)) {
            combined = current;
            previousSelected = null;
        }
        if (combined == null || combined instanceof VariantType) {
            if (current != null && !(current instanceof VariantType)) {
                combined = current;
            } else if (previousSelected != null) {
                combined = previousSelected;
            } else {
                return DataTypes.VARIANT();
            }
        }

        if (combined instanceof RowType) {
            RowType combinedRow = (RowType) combined;
            RowType currentRow = current instanceof RowType ? (RowType) current : null;
            RowType previousRow =
                    previousSelected instanceof RowType ? (RowType) previousSelected : null;
            List<DataField> candidates = new ArrayList<>();
            for (DataField field : combinedRow.getFields()) {
                DataField previousField = findField(previousRow, field.name());
                double threshold = previousField == null ? admissionRatio : retentionRatio;
                double ratio = rootValueCount == 0 ? 0 : getFieldCount(field) / rootValueCount;
                if (ratio >= threshold) {
                    candidates.add(field);
                }
            }
            candidates.sort(
                    Comparator.comparingDouble(
                                    (DataField field) ->
                                            rootValueCount == 0
                                                    ? 0
                                                    : getFieldCount(field) / rootValueCount)
                            .reversed()
                            .thenComparing(field -> findField(previousRow, field.name()) == null)
                            .thenComparing(DataField::name));

            List<DataField> selected = new ArrayList<>();
            for (DataField field : candidates) {
                if (maxFields.remaining <= 0) {
                    break;
                }
                DataField currentField = findField(currentRow, field.name());
                DataField previousField = findField(previousRow, field.name());
                DataType selectedType =
                        finalizeAdaptiveSchema(
                                field.type(),
                                currentField == null ? null : currentField.type(),
                                previousField == null ? null : previousField.type(),
                                rootValueCount,
                                admissionRatio,
                                retentionRatio,
                                maxFields);
                selected.add(new DataField(0, field.name(), selectedType));
            }
            selected.sort(Comparator.comparing(DataField::name));
            List<DataField> fields = new ArrayList<>();
            for (int i = 0; i < selected.size(); i++) {
                DataField field = selected.get(i);
                fields.add(new DataField(i, field.name(), field.type()));
            }
            return fields.isEmpty() ? DataTypes.VARIANT() : new RowType(fields);
        }

        if (combined instanceof ArrayType) {
            ArrayType combinedArray = (ArrayType) combined;
            DataType currentElement =
                    current instanceof ArrayType ? ((ArrayType) current).getElementType() : null;
            DataType previousElement =
                    previousSelected instanceof ArrayType
                            ? ((ArrayType) previousSelected).getElementType()
                            : null;
            return new ArrayType(
                    finalizeAdaptiveSchema(
                            combinedArray.getElementType(),
                            currentElement,
                            previousElement,
                            rootValueCount,
                            admissionRatio,
                            retentionRatio,
                            maxFields));
        }

        maxFields.remaining--;
        return selectScalarType(combined, current, previousSelected);
    }

    private DataType selectScalarType(
            DataType combined, DataType current, DataType previousSelected) {
        if (current == null) {
            return previousSelected == null ? combined : previousSelected;
        }
        if (previousSelected == null) {
            return widenScalarType(current);
        }

        DataType merged = mergeScalarTypes(previousSelected, current);
        return merged instanceof VariantType ? widenScalarType(current) : merged;
    }

    private boolean compatibleTypeFamilies(DataType previous, DataType current) {
        if (previous instanceof RowType || current instanceof RowType) {
            return previous instanceof RowType && current instanceof RowType;
        }
        if (previous instanceof ArrayType || current instanceof ArrayType) {
            return previous instanceof ArrayType && current instanceof ArrayType;
        }
        return !(mergeScalarTypes(previous, current) instanceof VariantType);
    }

    private DataType mergeScalarTypes(DataType first, DataType second) {
        if (first instanceof DecimalType && second instanceof DecimalType) {
            return mergeDecimal((DecimalType) first, (DecimalType) second);
        }
        if (first instanceof DecimalType
                && second.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT) {
            return mergeDecimalWithLong((DecimalType) first);
        }
        if (first.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT
                && second instanceof DecimalType) {
            return mergeDecimalWithLong((DecimalType) second);
        }
        return first.equals(second) ? first : DataTypes.VARIANT();
    }

    private DataType widenScalarType(DataType dataType) {
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            if (decimalType.getPrecision() <= 18 && decimalType.getScale() == 0) {
                return DataTypes.BIGINT();
            }
            return decimalType.getPrecision() <= 18
                    ? DataTypes.DECIMAL(18, decimalType.getScale())
                    : DataTypes.DECIMAL(DecimalType.MAX_PRECISION, decimalType.getScale());
        }
        return dataType;
    }

    private DataField findField(RowType rowType, String fieldName) {
        if (rowType == null) {
            return null;
        }
        for (DataField field : rowType.getFields()) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    /** Container for a mutable integer to track the total number of shredded fields. */
    private static class MaxFields {
        int remaining;

        MaxFields(int remaining) {
            this.remaining = remaining;
        }
    }

    static final class InferenceEvidence {

        private final Map<List<Integer>, ColumnEvidence> columns;

        private InferenceEvidence(Map<List<Integer>, ColumnEvidence> columns) {
            this.columns = columns;
        }
    }

    private static final class ColumnEvidence {

        private final double rootValueCount;
        private final DataType observedSchema;

        private ColumnEvidence(double rootValueCount, DataType observedSchema) {
            this.rootValueCount = rootValueCount;
            this.observedSchema = observedSchema;
        }
    }

    static final class AdaptiveInferenceResult {

        private final RowType physicalRowType;
        private final InferenceEvidence evidence;
        private final Map<List<Integer>, DataType> selectedSchemas;

        private AdaptiveInferenceResult(
                RowType physicalRowType,
                InferenceEvidence evidence,
                Map<List<Integer>, DataType> selectedSchemas) {
            this.physicalRowType = physicalRowType;
            this.evidence = evidence;
            this.selectedSchemas = selectedSchemas;
        }

        RowType physicalRowType() {
            return physicalRowType;
        }

        InferenceEvidence evidence() {
            return evidence;
        }

        Map<List<Integer>, DataType> selectedSchemas() {
            return selectedSchemas;
        }
    }

    /**
     * Given the schema of a Variant type, finalize the schema. Specifically: 1) Widen integer types
     * to LongType 2) Replace empty rows with VariantType 3) Limit the total number of shredded
     * fields in the schema
     */
    private DataType finalizeSimpleSchema(DataType dt, int minCardinality, MaxFields maxFields) {

        // Every field uses a value column
        maxFields.remaining--;
        if (maxFields.remaining <= 0) {
            return DataTypes.VARIANT();
        }

        // Handle null type first
        if (dt == null || dt instanceof VariantType) {
            return DataTypes.VARIANT();
        }

        if (dt instanceof RowType) {
            RowType rowType = (RowType) dt;
            List<DataField> newFields = new ArrayList<>();
            int fieldId = 0;

            for (DataField field : rowType.getFields()) {
                if (getFieldCount(field) >= minCardinality && maxFields.remaining > 0) {
                    DataType newType =
                            finalizeSimpleSchema(field.type(), minCardinality, maxFields);
                    // Clear description after finalizing
                    newFields.add(new DataField(fieldId++, field.name(), newType, null));
                }
            }

            if (!newFields.isEmpty()) {
                return new RowType(newFields);
            } else {
                return DataTypes.VARIANT();
            }
        } else if (dt instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dt;
            DataType newElementType =
                    finalizeSimpleSchema(arrayType.getElementType(), minCardinality, maxFields);
            return new ArrayType(newElementType);
        } else if (dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.TINYINT
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.SMALLINT
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.INTEGER
                || dt.getTypeRoot() == org.apache.paimon.types.DataTypeRoot.BIGINT) {
            maxFields.remaining--;
            return DataTypes.BIGINT();
        } else if (dt instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dt;
            if (decimalType.getPrecision() <= 18 && decimalType.getScale() == 0) {
                maxFields.remaining--;
                return DataTypes.BIGINT();
            } else {
                maxFields.remaining--;
                if (decimalType.getPrecision() <= 18) {
                    return DataTypes.DECIMAL(18, decimalType.getScale());
                } else {
                    return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, decimalType.getScale());
                }
            }
        } else {
            // All other scalar types use typed_value
            maxFields.remaining--;
            return dt;
        }
    }
}
