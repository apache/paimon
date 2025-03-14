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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.ReassignFieldId;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** A schema change message from the CDC source. */
public class CdcSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<DataField> fields;

    private final List<String> primaryKeys;

    @Nullable private final String comment;

    public CdcSchema(List<DataField> fields, List<String> primaryKeys, @Nullable String comment) {
        this.fields = fields;
        this.primaryKeys = primaryKeys;
        this.comment = comment;
    }

    public List<DataField> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public String comment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CdcSchema that = (CdcSchema) o;
        return Objects.equals(fields, that.fields)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields, primaryKeys, comment);
    }

    @Override
    public String toString() {
        return "Schema{"
                + "fields="
                + fields
                + ", primaryKeys="
                + primaryKeys
                + ", comment="
                + comment
                + '}';
    }

    /** Builder for configuring and creating instances of {@link CdcSchema}. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** A builder for constructing an immutable but still unresolved {@link CdcSchema}. */
    public static final class Builder {

        private final List<DataField> columns = new ArrayList<>();

        private List<String> primaryKeys = new ArrayList<>();

        @Nullable private String comment;

        private final AtomicInteger highestFieldId = new AtomicInteger(-1);

        public int getHighestFieldId() {
            return highestFieldId.get();
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * @param dataField data field
         */
        public Builder column(DataField dataField) {
            Preconditions.checkNotNull(dataField, "Data field must not be null.");
            Preconditions.checkNotNull(dataField.name(), "Column name must not be null.");
            Preconditions.checkNotNull(dataField.type(), "Data type must not be null.");
            columns.add(dataField);
            return this;
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         */
        public Builder column(String columnName, DataType dataType) {
            return column(columnName, dataType, null);
        }

        /**
         * Declares a column that is appended to this schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param description description of the column
         */
        public Builder column(String columnName, DataType dataType, @Nullable String description) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");

            int id = highestFieldId.incrementAndGet();
            DataType reassignDataType = ReassignFieldId.reassign(dataType, highestFieldId);
            columns.add(new DataField(id, columnName, reassignDataType, description));
            return this;
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a set of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            this.primaryKeys = new ArrayList<>(columnNames);
            return this;
        }

        /** Declares table comment. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an instance of an unresolved {@link CdcSchema}. */
        public CdcSchema build() {
            return new CdcSchema(columns, primaryKeys, comment);
        }
    }
}
