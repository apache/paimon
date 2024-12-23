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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaChanges {
    private static final String FIELD_SET_OPTIONS_NAME = "set-options";
    private static final String FIELD_REMOVE_OPTIONS_NAME = "remove-options";
    private static final String FIELD_COMMENT_NAME = "comment";
    private static final String FIELD_ADD_COLUMNS_NAME = "add-columns";
    private static final String FIELD_RENAME_COLUMNS_NAME = "rename-columns";

    private Map<String, String> setOptions;
    private List<String> removeOptions;
    private String comment;
    private List<SchemaChange.AddColumn> addColumns;
    private List<SchemaChange.RenameColumn> renameColumns;
    private List<SchemaChange.DropColumn> dropColumns;
    private List<SchemaChange.UpdateColumnType> updateColumnTypes;
    private List<SchemaChange.UpdateColumnNullability> updateColumnNullabilities;
    private List<SchemaChange.UpdateColumnComment> updateColumnComments;
    private List<SchemaChange.UpdateColumnPosition> updateColumnPositions;

    public SchemaChanges(
            Map<String, String> setOptions,
            List<String> removeOptions,
            String comment,
            List<SchemaChange.AddColumn> addColumns,
            List<SchemaChange.RenameColumn> renameColumns,
            List<SchemaChange.DropColumn> dropColumns,
            List<SchemaChange.UpdateColumnType> updateColumnTypes,
            List<SchemaChange.UpdateColumnNullability> updateColumnNullabilities,
            List<SchemaChange.UpdateColumnComment> updateColumnComments,
            List<SchemaChange.UpdateColumnPosition> updateColumnPositions) {
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
        Map<String, String> setOptions = null;
        List<String> removeOptions = new ArrayList<>();
        String comment = null;
        List<SchemaChange.AddColumn> addColumns = new ArrayList<>();
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
            }
        }
        this.setOptions = setOptions;
        this.removeOptions = removeOptions;
        this.comment = comment;
        this.addColumns = addColumns;
    }

    public Map<String, String> getSetOptions() {
        return setOptions;
    }

    public List<String> getRemoveOptions() {
        return removeOptions;
    }

    public String getComment() {
        return comment;
    }

    public List<SchemaChange.AddColumn> getAddColumns() {
        return addColumns;
    }

    public List<SchemaChange.RenameColumn> getRenameColumns() {
        return renameColumns;
    }

    public List<SchemaChange.DropColumn> getDropColumns() {
        return dropColumns;
    }

    public List<SchemaChange.UpdateColumnType> getUpdateColumnTypes() {
        return updateColumnTypes;
    }

    public List<SchemaChange.UpdateColumnNullability> getUpdateColumnNullabilities() {
        return updateColumnNullabilities;
    }

    public List<SchemaChange.UpdateColumnComment> getUpdateColumnComments() {
        return updateColumnComments;
    }

    public List<SchemaChange.UpdateColumnPosition> getUpdateColumnPositions() {
        return updateColumnPositions;
    }
}
