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

package org.apache.paimon.schema;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Schema change to table.
 *
 * @since 0.4.0
 */
@Public
public interface SchemaChange extends Serializable {

    static SchemaChange setOption(String key, String value) {
        return new SetOption(key, value);
    }

    static SchemaChange removeOption(String key) {
        return new RemoveOption(key);
    }

    static SchemaChange updateComment(@Nullable String comment) {
        return new UpdateComment(comment);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType) {
        return addColumn(fieldName, dataType, null, null);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType, String comment) {
        return new AddColumn(Collections.singletonList(fieldName), dataType, comment, null);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType, String comment, Move move) {
        return new AddColumn(Collections.singletonList(fieldName), dataType, comment, move);
    }

    static SchemaChange addColumn(
            List<String> fieldNames, DataType dataType, String comment, Move move) {
        return new AddColumn(fieldNames, dataType, comment, move);
    }

    static SchemaChange renameColumn(String fieldName, String newName) {
        return new RenameColumn(Collections.singletonList(fieldName), newName);
    }

    static SchemaChange renameColumn(List<String> fieldNames, String newName) {
        return new RenameColumn(fieldNames, newName);
    }

    static SchemaChange dropColumn(String fieldName) {
        return new DropColumn(Collections.singletonList(fieldName));
    }

    static SchemaChange dropColumn(List<String> fieldNames) {
        return new DropColumn(fieldNames);
    }

    static SchemaChange updateColumnType(String fieldName, DataType newDataType) {
        return new UpdateColumnType(fieldName, newDataType, false);
    }

    static SchemaChange updateColumnType(
            String fieldName, DataType newDataType, boolean keepNullability) {
        return new UpdateColumnType(fieldName, newDataType, keepNullability);
    }

    static SchemaChange updateColumnNullability(String fieldName, boolean newNullability) {
        return new UpdateColumnNullability(new String[] {fieldName}, newNullability);
    }

    static SchemaChange updateColumnNullability(String[] fieldNames, boolean newNullability) {
        return new UpdateColumnNullability(fieldNames, newNullability);
    }

    static SchemaChange updateColumnComment(String fieldName, String comment) {
        return new UpdateColumnComment(new String[] {fieldName}, comment);
    }

    static SchemaChange updateColumnComment(String[] fieldNames, String comment) {
        return new UpdateColumnComment(fieldNames, comment);
    }

    static SchemaChange updateColumnPosition(Move move) {
        return new UpdateColumnPosition(move);
    }

    /** A SchemaChange to set a table option. */
    final class SetOption implements SchemaChange {

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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

    /** A SchemaChange to Update table comment. */
    final class UpdateComment implements SchemaChange {

        private static final long serialVersionUID = 1L;

        // If comment is null, means to remove comment
        private final @Nullable String comment;

        private UpdateComment(@Nullable String comment) {
            this.comment = comment;
        }

        public @Nullable String comment() {
            return comment;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }
            UpdateComment that = (UpdateComment) object;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }
    }

    /** A SchemaChange to add a field. */
    final class AddColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private final List<String> fieldNames;
        private final DataType dataType;
        private final String description;
        private final Move move;

        private AddColumn(
                List<String> fieldNames, DataType dataType, String description, Move move) {
            this.fieldNames = fieldNames;
            this.dataType = dataType;
            this.description = description;
            this.move = move;
        }

        public List<String> fieldNames() {
            return fieldNames;
        }

        public DataType dataType() {
            return dataType;
        }

        @Nullable
        public String description() {
            return description;
        }

        @Nullable
        public Move move() {
            return move;
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
            return Objects.equals(fieldNames, addColumn.fieldNames)
                    && dataType.equals(addColumn.dataType)
                    && Objects.equals(description, addColumn.description)
                    && move.equals(addColumn.move);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(dataType, description);
            result = 31 * result + Objects.hashCode(fieldNames);
            result = 31 * result + Objects.hashCode(move);
            return result;
        }
    }

    /** A SchemaChange to rename a field. */
    final class RenameColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private final List<String> fieldNames;
        private final String newName;

        private RenameColumn(List<String> fieldNames, String newName) {
            this.fieldNames = fieldNames;
            this.newName = newName;
        }

        public List<String> fieldNames() {
            return fieldNames;
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
            return Objects.equals(fieldNames, that.fieldNames)
                    && Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newName);
            result = 31 * result + Objects.hashCode(fieldNames);
            return result;
        }
    }

    /** A SchemaChange to drop a field. */
    final class DropColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private final List<String> fieldNames;

        private DropColumn(List<String> fieldNames) {
            this.fieldNames = fieldNames;
        }

        public List<String> fieldNames() {
            return fieldNames;
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
            return Objects.equals(fieldNames, that.fieldNames);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldNames);
        }
    }

    /** A SchemaChange to update the field type. */
    final class UpdateColumnType implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private final String fieldName;
        private final DataType newDataType;
        // If true, do not change the target field nullability
        private final boolean keepNullability;

        private UpdateColumnType(String fieldName, DataType newDataType, boolean keepNullability) {
            this.fieldName = fieldName;
            this.newDataType = newDataType;
            this.keepNullability = keepNullability;
        }

        public String fieldName() {
            return fieldName;
        }

        public DataType newDataType() {
            return newDataType;
        }

        public boolean keepNullability() {
            return keepNullability;
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

    /** A SchemaChange to update the field position. */
    final class UpdateColumnPosition implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private final Move move;

        private UpdateColumnPosition(Move move) {
            this.move = move;
        }

        public Move move() {
            return move;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UpdateColumnPosition updateColumnPosition = (UpdateColumnPosition) o;
            return Objects.equals(move, updateColumnPosition.move);
        }

        @Override
        public int hashCode() {
            return Objects.hash(move);
        }
    }

    /** Represents a requested column move in a struct. */
    class Move implements Serializable {

        public enum MoveType {
            FIRST,
            AFTER,
            BEFORE,
            LAST
        }

        public static Move first(String fieldName) {
            return new Move(fieldName, null, MoveType.FIRST);
        }

        public static Move after(String fieldName, String referenceFieldName) {
            return new Move(fieldName, referenceFieldName, MoveType.AFTER);
        }

        public static Move before(String fieldName, String referenceFieldName) {
            return new Move(fieldName, referenceFieldName, MoveType.BEFORE);
        }

        public static Move last(String fieldName) {
            return new Move(fieldName, null, MoveType.LAST);
        }

        private static final long serialVersionUID = 1L;

        private final String fieldName;
        private final String referenceFieldName;
        private final MoveType type;

        public Move(String fieldName, String referenceFieldName, MoveType type) {
            this.fieldName = fieldName;
            this.referenceFieldName = referenceFieldName;
            this.type = type;
        }

        public String fieldName() {
            return fieldName;
        }

        public String referenceFieldName() {
            return referenceFieldName;
        }

        public MoveType type() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Move move = (Move) o;
            return Objects.equals(fieldName, move.fieldName)
                    && Objects.equals(referenceFieldName, move.referenceFieldName)
                    && Objects.equals(type, move.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, referenceFieldName, type);
        }
    }

    /** A SchemaChange to update the (nested) field nullability. */
    final class UpdateColumnNullability implements SchemaChange {

        private static final long serialVersionUID = 1L;

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

        private static final long serialVersionUID = 1L;

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
