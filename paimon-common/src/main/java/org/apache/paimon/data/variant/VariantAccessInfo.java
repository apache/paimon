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

import org.apache.paimon.types.DataField;

import java.io.Serializable;
import java.util.List;

/** Variant access information for a variant column. */
public class VariantAccessInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    // The name of the variant column.
    private final String columnName;

    // Extracted fields from the variant.
    private final List<VariantField> variantFields;

    public VariantAccessInfo(String columnName, List<VariantField> variantFields) {
        this.columnName = columnName;
        this.variantFields = variantFields;
    }

    public String columnName() {
        return columnName;
    }

    public List<VariantField> variantFields() {
        return variantFields;
    }

    @Override
    public String toString() {
        return "VariantAccessInfo{"
                + "columnName='"
                + columnName
                + '\''
                + ", variantFields="
                + variantFields
                + '}';
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
    }
}
