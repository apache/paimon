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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.types.DataField;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Variant extraction information that describes fields extraction from a variant column. */
@Experimental
public class VariantExtraction implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Returns the path to the variant column. For top-level variant columns, this is a single
     * element array containing the column name. For nested variant columns within structs, this is
     * an array representing the path (e.g., ["structCol", "innerStruct", "variantCol"]).
     */
    private final String[] columnName;

    // Extracted fields from the variant.
    private final List<VariantField> variantFields;

    public VariantExtraction(String columnName, List<VariantField> variantFields) {
        this.columnName = new String[] {columnName};
        this.variantFields = variantFields;
    }

    public String[] columnName() {
        return columnName;
    }

    public List<VariantField> variantFields() {
        return variantFields;
    }

    @Override
    public String toString() {
        return "VariantExtraction{"
                + "columnName="
                + Arrays.toString(columnName)
                + ", variantFields="
                + variantFields
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantExtraction that = (VariantExtraction) o;
        return Objects.deepEquals(columnName, that.columnName)
                && Objects.equals(variantFields, that.variantFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(columnName), variantFields);
    }

    /** Variant field extracted from the variant. */
    public static class VariantField implements Serializable {

        private static final long serialVersionUID = 1L;

        // The data field of the variant field.
        private final DataField dataField;

        // The `path` parameter of VariantGet.
        private final String path;

        private final VariantCastArgs castArgs;

        public VariantField(DataField dataField, String path, VariantCastArgs castArgs) {
            this.dataField = dataField;
            this.path = path;
            this.castArgs = castArgs;
        }

        public VariantField(DataField dataField, String path) {
            this(dataField, path, VariantCastArgs.defaultArgs());
        }

        public DataField dataField() {
            return dataField;
        }

        public String path() {
            return path;
        }

        public VariantCastArgs castArgs() {
            return castArgs;
        }

        @Override
        public String toString() {
            return "VariantField{"
                    + "dataField="
                    + dataField
                    + ", path='"
                    + path
                    + '\''
                    + ", castArgs="
                    + castArgs
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            VariantField that = (VariantField) o;
            return Objects.equals(dataField, that.dataField)
                    && Objects.equals(path, that.path)
                    && Objects.equals(castArgs, that.castArgs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataField, path, castArgs);
        }
    }
}
