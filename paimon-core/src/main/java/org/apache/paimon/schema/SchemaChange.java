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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Schema change to table.
 *
 * @since 0.4.0
 */
@Public
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = SchemaChange.Actions.FIELD_ACTION)
@JsonSubTypes({
    @JsonSubTypes.Type(
            value = SchemaChange.SetOption.class,
            name = SchemaChange.Actions.SET_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.RemoveOption.class,
            name = SchemaChange.Actions.REMOVE_OPTION_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateComment.class,
            name = SchemaChange.Actions.UPDATE_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.AddColumn.class,
            name = SchemaChange.Actions.ADD_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.RenameColumn.class,
            name = SchemaChange.Actions.RENAME_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.DropColumn.class,
            name = SchemaChange.Actions.DROP_COLUMN_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnType.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_TYPE_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnNullability.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_NULLABILITY_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnComment.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_COMMENT_ACTION),
    @JsonSubTypes.Type(
            value = SchemaChange.UpdateColumnPosition.class,
            name = SchemaChange.Actions.UPDATE_COLUMN_POSITION_ACTION),
})
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
        return new AddColumn(new String[] {fieldName}, dataType, comment, null);
    }

    static SchemaChange addColumn(String fieldName, DataType dataType, String comment, Move move) {
        return new AddColumn(new String[] {fieldName}, dataType, comment, move);
    }

    static SchemaChange addColumn(
            String[] fieldNames, DataType dataType, String comment, Move move) {
        return new AddColumn(fieldNames, dataType, comment, move);
    }

    static SchemaChange renameColumn(String fieldName, String newName) {
        return new RenameColumn(new String[] {fieldName}, newName);
    }

    static SchemaChange renameColumn(String[] fieldNames, String newName) {
        return new RenameColumn(fieldNames, newName);
    }

    static SchemaChange dropColumn(String fieldName) {
        return new DropColumn(new String[] {fieldName});
    }

    static SchemaChange dropColumn(String[] fieldNames) {
        return new DropColumn(fieldNames);
    }

    static SchemaChange updateColumnType(String fieldName, DataType newDataType) {
        return new UpdateColumnType(new String[] {fieldName}, newDataType, false);
    }

    static SchemaChange updateColumnType(
            String fieldName, DataType newDataType, boolean keepNullability) {
        return new UpdateColumnType(new String[] {fieldName}, newDataType, keepNullability);
    }

    static SchemaChange updateColumnType(
            String[] fieldNames, DataType newDataType, boolean keepNullability) {
        return new UpdateColumnType(fieldNames, newDataType, keepNullability);
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

        private static final String FIELD_KEY = "key";
        private static final String FIELD_VALUE = "value";

        @JsonProperty(FIELD_KEY)
        private final String key;

        @JsonProperty(FIELD_VALUE)
        private final String value;

        @JsonCreator
        private SetOption(
                @JsonProperty(FIELD_KEY) String key, @JsonProperty(FIELD_VALUE) String value) {
            this.key = key;
            this.value = value;
        }

        @JsonGetter(FIELD_KEY)
        public String key() {
            return key;
        }

        @JsonGetter(FIELD_VALUE)
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

        private static final String FIELD_KEY = "key";

        @JsonProperty(FIELD_KEY)
        private final String key;

        private RemoveOption(@JsonProperty(FIELD_KEY) String key) {
            this.key = key;
        }

        @JsonGetter(FIELD_KEY)
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

        private static final String FIELD_COMMENT = "comment";

        // If comment is null, means to remove comment
        @JsonProperty(FIELD_COMMENT)
        private final @Nullable String comment;

        private UpdateComment(@JsonProperty(FIELD_COMMENT) @Nullable String comment) {
            this.comment = comment;
        }

        @JsonGetter(FIELD_COMMENT)
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
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class AddColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_DATA_TYPE = "dataType";
        private static final String FIELD_COMMENT = "comment";
        private static final String FIELD_MOVE = "move";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_DATA_TYPE)
        private final DataType dataType;

        @JsonProperty(FIELD_COMMENT)
        private final String description;

        @JsonProperty(FIELD_MOVE)
        private final Move move;

        @JsonCreator
        private AddColumn(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_DATA_TYPE) DataType dataType,
                @JsonProperty(FIELD_COMMENT) String description,
                @JsonProperty(FIELD_MOVE) Move move) {
            this.fieldNames = fieldNames;
            this.dataType = dataType;
            this.description = description;
            this.move = move;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_DATA_TYPE)
        public DataType dataType() {
            return dataType;
        }

        @Nullable
        @JsonGetter(FIELD_COMMENT)
        public String description() {
            return description;
        }

        @Nullable
        @JsonGetter(FIELD_MOVE)
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
            return Arrays.equals(fieldNames, addColumn.fieldNames)
                    && dataType.equals(addColumn.dataType)
                    && Objects.equals(description, addColumn.description)
                    && Objects.equals(move, addColumn.move);
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
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class RenameColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_NAME = "newName";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_NAME)
        private final String newName;

        @JsonCreator
        private RenameColumn(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_NAME) String newName) {
            this.fieldNames = fieldNames;
            this.newName = newName;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_NAME)
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
            return Arrays.equals(fieldNames, that.fieldNames)
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
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class DropColumn implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonCreator
        private DropColumn(@JsonProperty(FIELD_FILED_NAMES) String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
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
            return Arrays.equals(fieldNames, that.fieldNames);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldNames);
        }
    }

    /** A SchemaChange to update the field type. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnType implements SchemaChange {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_DATA_TYPE = "newDataType";
        private static final String FIELD_KEEP_NULLABILITY = "keepNullability";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_DATA_TYPE)
        private final DataType newDataType;
        // If true, do not change the target field nullability
        @JsonProperty(FIELD_KEEP_NULLABILITY)
        private final boolean keepNullability;

        @JsonCreator
        private UpdateColumnType(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_DATA_TYPE) DataType newDataType,
                @JsonProperty(FIELD_KEEP_NULLABILITY) boolean keepNullability) {
            this.fieldNames = fieldNames;
            this.newDataType = newDataType;
            this.keepNullability = keepNullability;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_DATA_TYPE)
        public DataType newDataType() {
            return newDataType;
        }

        @JsonGetter(FIELD_KEEP_NULLABILITY)
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
            return Arrays.equals(fieldNames, that.fieldNames)
                    && newDataType.equals(that.newDataType);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(newDataType);
            result = 31 * result + Objects.hashCode(fieldNames);
            return result;
        }
    }

    /** A SchemaChange to update the field position. */
    final class UpdateColumnPosition implements SchemaChange {

        private static final long serialVersionUID = 1L;
        private static final String FIELD_MOVE = "move";

        @JsonProperty(FIELD_MOVE)
        private final Move move;

        @JsonCreator
        private UpdateColumnPosition(@JsonProperty(FIELD_MOVE) Move move) {
            this.move = move;
        }

        @JsonGetter(FIELD_MOVE)
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
    @JsonIgnoreProperties(ignoreUnknown = true)
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

        private static final String FIELD_FILED_NAME = "fieldName";
        private static final String FIELD_REFERENCE_FIELD_NAME = "referenceFieldName";
        private static final String FIELD_TYPE = "type";

        @JsonProperty(FIELD_FILED_NAME)
        private final String fieldName;

        @JsonProperty(FIELD_REFERENCE_FIELD_NAME)
        private final String referenceFieldName;

        @JsonProperty(FIELD_TYPE)
        private final MoveType type;

        @JsonCreator
        public Move(
                @JsonProperty(FIELD_FILED_NAME) String fieldName,
                @JsonProperty(FIELD_REFERENCE_FIELD_NAME) String referenceFieldName,
                @JsonProperty(FIELD_TYPE) MoveType type) {
            this.fieldName = fieldName;
            this.referenceFieldName = referenceFieldName;
            this.type = type;
        }

        @JsonGetter(FIELD_FILED_NAME)
        public String fieldName() {
            return fieldName;
        }

        @JsonGetter(FIELD_REFERENCE_FIELD_NAME)
        public String referenceFieldName() {
            return referenceFieldName;
        }

        @JsonGetter(FIELD_TYPE)
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
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnNullability implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_NULLABILITY = "newNullability";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_NULLABILITY)
        private final boolean newNullability;

        @JsonCreator
        public UpdateColumnNullability(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_NULLABILITY) boolean newNullability) {
            this.fieldNames = fieldNames;
            this.newNullability = newNullability;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_NULLABILITY)
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
    @JsonIgnoreProperties(ignoreUnknown = true)
    final class UpdateColumnComment implements SchemaChange {

        private static final long serialVersionUID = 1L;

        private static final String FIELD_FILED_NAMES = "fieldNames";
        private static final String FIELD_NEW_COMMENT = "newComment";

        @JsonProperty(FIELD_FILED_NAMES)
        private final String[] fieldNames;

        @JsonProperty(FIELD_NEW_COMMENT)
        private final String newDescription;

        @JsonCreator
        public UpdateColumnComment(
                @JsonProperty(FIELD_FILED_NAMES) String[] fieldNames,
                @JsonProperty(FIELD_NEW_COMMENT) String newDescription) {
            this.fieldNames = fieldNames;
            this.newDescription = newDescription;
        }

        @JsonGetter(FIELD_FILED_NAMES)
        public String[] fieldNames() {
            return fieldNames;
        }

        @JsonGetter(FIELD_NEW_COMMENT)
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

    /** Actions for schema changes： identify for schema change. */
    public static class Actions {
        public static final String FIELD_ACTION = "action";
        public static final String SET_OPTION_ACTION = "setOption";
        public static final String REMOVE_OPTION_ACTION = "removeOption";
        public static final String UPDATE_COMMENT_ACTION = "updateComment";
        public static final String ADD_COLUMN_ACTION = "addColumn";
        public static final String RENAME_COLUMN_ACTION = "renameColumn";
        public static final String DROP_COLUMN_ACTION = "dropColumn";
        public static final String UPDATE_COLUMN_TYPE_ACTION = "updateColumnType";
        public static final String UPDATE_COLUMN_NULLABILITY_ACTION = "updateColumnNullability";
        public static final String UPDATE_COLUMN_COMMENT_ACTION = "updateColumnComment";
        public static final String UPDATE_COLUMN_POSITION_ACTION = "updateColumnPosition";
    }
}
