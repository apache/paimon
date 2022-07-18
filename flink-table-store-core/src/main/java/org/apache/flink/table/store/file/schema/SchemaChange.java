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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.Objects;

/** Schema change to table. */
public interface SchemaChange {

    // TODO more change support like updateColumnNullability and updateColumnComment.

    static SchemaChange setOption(String key, String value) {
        return new SetOption(key, value);
    }

    static SchemaChange removeOption(String key) {
        return new RemoveOption(key);
    }

    static SchemaChange addColumn(String fieldName, LogicalType logicalType) {
        return addColumn(fieldName, logicalType, true, null);
    }

    static SchemaChange addColumn(
            String fieldName, LogicalType logicalType, boolean isNullable, String comment) {
        return new AddColumn(fieldName, logicalType, isNullable, comment);
    }

    static SchemaChange updateColumnType(String fieldName, LogicalType newLogicalType) {
        return new UpdateColumnType(fieldName, newLogicalType);
    }

    /** A SchemaChange to set a table option. */
    final class SetOption implements SchemaChange {
        private final String key;
        private final String value;

        private SetOption(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SetOption that = (SetOption) o;
            return key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }

    /** A SchemaChange to remove a table option. */
    final class RemoveOption implements SchemaChange {
        private final String key;

        private RemoveOption(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemoveOption that = (RemoveOption) o;
            return key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }
    }

    /** A SchemaChange to add a field. */
    final class AddColumn implements SchemaChange {
        private final String fieldName;
        private final LogicalType logicalType;
        private final boolean isNullable;
        private final String description;

        private AddColumn(
                String fieldName, LogicalType logicalType, boolean isNullable, String description) {
            this.fieldName = fieldName;
            this.logicalType = logicalType;
            this.isNullable = isNullable;
            this.description = description;
        }

        public String fieldName() {
            return fieldName;
        }

        public LogicalType logicalType() {
            return logicalType;
        }

        public boolean isNullable() {
            return isNullable;
        }

        @Nullable
        public String description() {
            return description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AddColumn addColumn = (AddColumn) o;
            return isNullable == addColumn.isNullable
                    && Objects.equals(fieldName, addColumn.fieldName)
                    && logicalType.equals(addColumn.logicalType)
                    && Objects.equals(description, addColumn.description);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(logicalType, isNullable, description);
            result = 31 * result + Objects.hashCode(fieldName);
            return result;
        }
    }

    /** A SchemaChange to update the field type. */
    final class UpdateColumnType implements SchemaChange {
        private final String fieldName;
        private final LogicalType newLogicalType;

        private UpdateColumnType(String fieldName, LogicalType newLogicalType) {
            this.fieldName = fieldName;
            this.newLogicalType = newLogicalType;
        }

        public String fieldName() {
            return fieldName;
        }

        public LogicalType newLogicalType() {
            return newLogicalType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UpdateColumnType that = (UpdateColumnType) o;
            return Objects.equals(fieldName, that.fieldName)
                    && newLogicalType.equals(that.newLogicalType);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newLogicalType);
            result = 31 * result + Objects.hashCode(fieldName);
            return result;
        }
    }
}
