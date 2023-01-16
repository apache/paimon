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

import org.apache.flink.table.store.types.DataType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

/** Schema change to table. */
public interface SchemaChange {

    static SchemaChange setOption(String key, String value) {
        return new SetOption(key, value);
    }

    static SchemaChange removeOption(String key) {
        return new RemoveOption(key);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType) {
        return addColumn(fieldName, dataType, null);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType, String comment) {
        return new AddColumn(fieldName, dataType, comment);
    }

    static SchemaChange renameColumn(String fieldName, String newName) {
        return new RenameColumn(fieldName, newName);
    }

    static SchemaChange dropColumn(String fieldName) {
        return new DropColumn(fieldName);
    }

    static SchemaChange updateColumnType(String fieldName, DataType newDataType) {
        return new UpdateColumnType(fieldName, newDataType);
    }

    static SchemaChange updateColumnNullability(String[] fieldNames, boolean newNullability) {
        return new UpdateColumnNullability(fieldNames, newNullability);
    }

    static SchemaChange updateColumnComment(String[] fieldNames, String comment) {
        return new UpdateColumnComment(fieldNames, comment);
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
        private final DataType dataType;
        private final String description;

        private AddColumn(String fieldName, DataType dataType, String description) {
            this.fieldName = fieldName;
            this.dataType = dataType;
            this.description = description;
        }

        public String fieldName() {
            return fieldName;
        }

        public DataType dataType() {
            return dataType;
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
            return Objects.equals(fieldName, addColumn.fieldName)
                    && dataType.equals(addColumn.dataType)
                    && Objects.equals(description, addColumn.description);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataType, description);
            result = 31 * result + Objects.hashCode(fieldName);
            return result;
        }
    }

    /** A SchemaChange to rename a field. */
    final class RenameColumn implements SchemaChange {
        private final String fieldName;
        private final String newName;

        private RenameColumn(String fieldName, String newName) {
            this.fieldName = fieldName;
            this.newName = newName;
        }

        public String fieldName() {
            return fieldName;
        }

        public String newName() {
            return newName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RenameColumn that = (RenameColumn) o;
            return Objects.equals(fieldName, that.fieldName)
                    && Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newName);
            result = 31 * result + Objects.hashCode(fieldName);
            return result;
        }
    }

    /** A SchemaChange to drop a field. */
    final class DropColumn implements SchemaChange {
        private final String fieldName;

        private DropColumn(String fieldName) {
            this.fieldName = fieldName;
        }

        public String fieldName() {
            return fieldName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DropColumn that = (DropColumn) o;
            return Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldName);
        }
    }

    /** A SchemaChange to update the field type. */
    final class UpdateColumnType implements SchemaChange {
        private final String fieldName;
        private final DataType newDataType;

        private UpdateColumnType(String fieldName, DataType newDataType) {
            this.fieldName = fieldName;
            this.newDataType = newDataType;
        }

        public String fieldName() {
            return fieldName;
        }

        public DataType newDataType() {
            return newDataType;
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
                    && newDataType.equals(that.newDataType);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDataType);
            result = 31 * result + Objects.hashCode(fieldName);
            return result;
        }
    }

    /** A SchemaChange to update the (nested) field nullability. */
    final class UpdateColumnNullability implements SchemaChange {
        private final String[] fieldNames;
        private final boolean newNullability;

        public UpdateColumnNullability(String[] fieldNames, boolean newNullability) {
            this.fieldNames = fieldNames;
            this.newNullability = newNullability;
        }

        public String[] fieldNames() {
            return fieldNames;
        }

        public boolean newNullability() {
            return newNullability;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UpdateColumnNullability)) {
                return false;
            }
            UpdateColumnNullability that = (UpdateColumnNullability) o;
            return newNullability == that.newNullability
                    && Arrays.equals(fieldNames, that.fieldNames);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newNullability);
            result = 31 * result + Arrays.hashCode(fieldNames);
            return result;
        }
    }

    /** A SchemaChange to update the (nested) field comment. */
    final class UpdateColumnComment implements SchemaChange {
        private final String[] fieldNames;
        private final String newDescription;

        public UpdateColumnComment(String[] fieldNames, String newDescription) {
            this.fieldNames = fieldNames;
            this.newDescription = newDescription;
        }

        public String[] fieldNames() {
            return fieldNames;
        }

        public String newDescription() {
            return newDescription;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UpdateColumnComment)) {
                return false;
            }
            UpdateColumnComment that = (UpdateColumnComment) o;
            return Arrays.equals(fieldNames, that.fieldNames)
                    && newDescription.equals(that.newDescription);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDescription);
            result = 31 * result + Arrays.hashCode(fieldNames);
            return result;
        }
    }
}
