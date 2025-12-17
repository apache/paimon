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

package org.apache.paimon.format.parquet.type;

import org.apache.paimon.data.variant.VariantAccessInfo;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Field that represent parquet's field type. */
public abstract class ParquetField {

    private final DataType type;
    private final int repetitionLevel;
    private final int definitionLevel;
    private final boolean required;
    private final String[] path;
    // When `variantFileType` has value, the parquet field should produce a variant type, and
    // `variantFileType` describes the file schema of the Parquet variant field.
    @Nullable private final ParquetField variantFileType;
    // Represent the required variant fields.
    @Nullable List<VariantAccessInfo.VariantField> variantFields;

    public ParquetField(
            DataType type,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            String[] path) {
        this(type, repetitionLevel, definitionLevel, required, path, null, null);
    }

    public ParquetField(
            DataType type,
            int repetitionLevel,
            int definitionLevel,
            boolean required,
            String[] path,
            @Nullable ParquetField variantFileType,
            @Nullable List<VariantAccessInfo.VariantField> variantFields) {
        this.type = type;
        this.repetitionLevel = repetitionLevel;
        this.definitionLevel = definitionLevel;
        this.required = required;
        this.path = path;
        this.variantFileType = variantFileType;
        this.variantFields = variantFields;
    }

    public DataType getType() {
        return type;
    }

    public int getRepetitionLevel() {
        return repetitionLevel;
    }

    public int getDefinitionLevel() {
        return definitionLevel;
    }

    public boolean isRequired() {
        return required;
    }

    public String[] path() {
        return path;
    }

    public Optional<ParquetField> variantFileType() {
        return Optional.ofNullable(variantFileType);
    }

    @Nullable
    public List<VariantAccessInfo.VariantField> variantFields() {
        return variantFields;
    }

    public abstract boolean isPrimitive();

    @Override
    public String toString() {
        return "ParquetField{"
                + "type="
                + type
                + ", repetitionLevel="
                + repetitionLevel
                + ", definitionLevel="
                + definitionLevel
                + ", required="
                + required
                + ", path="
                + Arrays.toString(path)
                + ", variantFileType="
                + variantFileType
                + ", variantFields="
                + variantFields
                + '}';
    }
}
