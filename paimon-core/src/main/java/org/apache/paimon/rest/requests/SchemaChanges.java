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

package org.apache.paimon.rest.requests;

import org.apache.paimon.schema.SchemaChange;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Schema changes to serialize List of SchemaChange . */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaChanges {

    private static final String FIELD_SET_OPTIONS = "set-options";
    private static final String FIELD_REMOVE_OPTIONS = "remove-options";
    private static final String FIELD_COMMENT = "comment";
    private static final String FIELD_ADD_COLUMNS = "add-columns";
    private static final String FIELD_RENAME_COLUMNS = "rename-columns";
    private static final String FIELD_DROP_COLUMNS = "drop-columns";
    private static final String FIELD_UPDATE_COLUMN_TYPES = "update-column-types";
    private static final String FIELD_UPDATE_COLUMN_NULLABILITIES = "update-column-nullabilities";
    private static final String FIELD_UPDATE_COLUMN_COMMENTS = "update-column-comments";
    private static final String FIELD_UPDATE_COLUMN_POSITIONS = "update-column-positions";

    @JsonProperty(FIELD_SET_OPTIONS)
    private Map<String, String> setOptions;

    @JsonProperty(FIELD_REMOVE_OPTIONS)
    private List<String> removeOptions;

    @JsonProperty(FIELD_COMMENT)
    private String comment;

    @JsonProperty(FIELD_ADD_COLUMNS)
    private List<SchemaChange.AddColumn> addColumns;

    @JsonProperty(FIELD_RENAME_COLUMNS)
    private List<SchemaChange.RenameColumn> renameColumns;

    @JsonProperty(FIELD_DROP_COLUMNS)
    private List<String> dropColumns;

    @JsonProperty(FIELD_UPDATE_COLUMN_TYPES)
    private List<SchemaChange.UpdateColumnType> updateColumnTypes;

    @JsonProperty(FIELD_UPDATE_COLUMN_NULLABILITIES)
    private List<SchemaChange.UpdateColumnNullability> updateColumnNullabilities;

    @JsonProperty(FIELD_UPDATE_COLUMN_COMMENTS)
    private List<SchemaChange.UpdateColumnComment> updateColumnComments;

    @JsonProperty(FIELD_UPDATE_COLUMN_POSITIONS)
    private List<SchemaChange.Move> updateColumnPositions;

    @JsonCreator
    public SchemaChanges(
            @JsonProperty(FIELD_SET_OPTIONS) Map<String, String> setOptions,
            @JsonProperty(FIELD_REMOVE_OPTIONS) List<String> removeOptions,
            @JsonProperty(FIELD_COMMENT) String comment,
            @JsonProperty(FIELD_ADD_COLUMNS) List<SchemaChange.AddColumn> addColumns,
            @JsonProperty(FIELD_RENAME_COLUMNS) List<SchemaChange.RenameColumn> renameColumns,
            @JsonProperty(FIELD_DROP_COLUMNS) List<String> dropColumns,
            @JsonProperty(FIELD_UPDATE_COLUMN_TYPES)
                    List<SchemaChange.UpdateColumnType> updateColumnTypes,
            @JsonProperty(FIELD_UPDATE_COLUMN_NULLABILITIES)
                    List<SchemaChange.UpdateColumnNullability> updateColumnNullabilities,
            @JsonProperty(FIELD_UPDATE_COLUMN_COMMENTS)
                    List<SchemaChange.UpdateColumnComment> updateColumnComments,
            @JsonProperty(FIELD_UPDATE_COLUMN_POSITIONS)
                    List<SchemaChange.Move> updateColumnPositions) {
        this.setOptions = setOptions;
        this.removeOptions = removeOptions;
        this.comment = comment;
        this.addColumns = addColumns;
        this.renameColumns = renameColumns;
        this.dropColumns = dropColumns;
        this.updateColumnTypes = updateColumnTypes;
        this.updateColumnNullabilities = updateColumnNullabilities;
        this.updateColumnComments = updateColumnComments;
        this.updateColumnPositions = updateColumnPositions;
    }

    public SchemaChanges(List<SchemaChange> changes) {
        Map<String, String> setOptions = new HashMap<>();
        List<String> removeOptions = new ArrayList<>();
        String comment = null;
        List<SchemaChange.AddColumn> addColumns = new ArrayList<>();
        List<SchemaChange.RenameColumn> renameColumns = new ArrayList<>();
        List<String> dropColumns = new ArrayList<>();
        List<SchemaChange.UpdateColumnType> updateColumnTypes = new ArrayList<>();
        List<SchemaChange.UpdateColumnNullability> updateColumnNullabilities = new ArrayList<>();
        List<SchemaChange.UpdateColumnComment> updateColumnComments = new ArrayList<>();
        List<SchemaChange.Move> updateColumnPositions = new ArrayList<>();
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.SetOption) {
                setOptions.put(
                        ((SchemaChange.SetOption) change).key(),
                        ((SchemaChange.SetOption) change).value());
            } else if (change instanceof SchemaChange.RemoveOption) {
                removeOptions.add(((SchemaChange.RemoveOption) change).key());
            } else if (change instanceof SchemaChange.UpdateComment) {
                comment = ((SchemaChange.UpdateComment) change).comment();
            } else if (change instanceof SchemaChange.AddColumn) {
                addColumns.add((SchemaChange.AddColumn) change);
            } else if (change instanceof SchemaChange.RenameColumn) {
                renameColumns.add((SchemaChange.RenameColumn) change);
            } else if (change instanceof SchemaChange.DropColumn) {
                dropColumns.addAll(Arrays.asList(((SchemaChange.DropColumn) change).fieldNames()));
            } else if (change instanceof SchemaChange.UpdateColumnType) {
                updateColumnTypes.add((SchemaChange.UpdateColumnType) change);
            } else if (change instanceof SchemaChange.UpdateColumnNullability) {
                updateColumnNullabilities.add((SchemaChange.UpdateColumnNullability) change);
            } else if (change instanceof SchemaChange.UpdateColumnComment) {
                updateColumnComments.add((SchemaChange.UpdateColumnComment) change);
            } else if (change instanceof SchemaChange.UpdateColumnPosition) {
                updateColumnPositions.add(((SchemaChange.UpdateColumnPosition) change).move());
            }
        }
        this.setOptions = setOptions;
        this.removeOptions = removeOptions;
        this.comment = comment;
        this.addColumns = addColumns;
        this.renameColumns = renameColumns;
        this.dropColumns = dropColumns;
        this.updateColumnTypes = updateColumnTypes;
        this.updateColumnNullabilities = updateColumnNullabilities;
        this.updateColumnComments = updateColumnComments;
        this.updateColumnPositions = updateColumnPositions;
    }

    @JsonGetter(FIELD_SET_OPTIONS)
    public Map<String, String> getSetOptions() {
        return setOptions;
    }

    @JsonGetter(FIELD_REMOVE_OPTIONS)
    public List<String> getRemoveOptions() {
        return removeOptions;
    }

    @JsonGetter(FIELD_COMMENT)
    public String getComment() {
        return comment;
    }

    @JsonGetter(FIELD_ADD_COLUMNS)
    public List<SchemaChange.AddColumn> getAddColumns() {
        return addColumns;
    }

    @JsonGetter(FIELD_RENAME_COLUMNS)
    public List<SchemaChange.RenameColumn> getRenameColumns() {
        return renameColumns;
    }

    @JsonGetter(FIELD_DROP_COLUMNS)
    public List<String> getDropColumns() {
        return dropColumns;
    }

    @JsonGetter(FIELD_UPDATE_COLUMN_TYPES)
    public List<SchemaChange.UpdateColumnType> getUpdateColumnTypes() {
        return updateColumnTypes;
    }

    @JsonGetter(FIELD_UPDATE_COLUMN_NULLABILITIES)
    public List<SchemaChange.UpdateColumnNullability> getUpdateColumnNullabilities() {
        return updateColumnNullabilities;
    }

    @JsonGetter(FIELD_UPDATE_COLUMN_COMMENTS)
    public List<SchemaChange.UpdateColumnComment> getUpdateColumnComments() {
        return updateColumnComments;
    }

    @JsonGetter(FIELD_UPDATE_COLUMN_POSITIONS)
    public List<SchemaChange.Move> getUpdateColumnPositions() {
        return updateColumnPositions;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaChanges that = (SchemaChanges) o;
        return Objects.equals(setOptions, that.setOptions)
                && Objects.equals(removeOptions, that.removeOptions)
                && Objects.equals(comment, that.comment)
                && Objects.equals(addColumns, that.addColumns)
                && Objects.equals(renameColumns, that.renameColumns)
                && Objects.equals(dropColumns, that.dropColumns)
                && Objects.equals(updateColumnTypes, that.updateColumnTypes)
                && Objects.equals(updateColumnNullabilities, that.updateColumnNullabilities)
                && Objects.equals(updateColumnComments, that.updateColumnComments)
                && Objects.equals(updateColumnPositions, that.updateColumnPositions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                setOptions,
                removeOptions,
                comment,
                addColumns,
                renameColumns,
                dropColumns,
                updateColumnTypes,
                updateColumnNullabilities,
                updateColumnComments,
                updateColumnPositions);
    }
}
