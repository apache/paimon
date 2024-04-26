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

package org.apache.paimon.fileindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Options of file index column. */
public class FileIndexOptions {

    public static final String FILE_INDEX = "file-index";

    public static final String COLUMNS = "columns";

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long fileIndexInManifestThreshold;

    private final Map<Column, Map<String, Options>> indexTypeOptions;
    private final Map<Column, Map<String, Options>> topLevelMapColumnOptions;

    public FileIndexOptions() {
        this(new CoreOptions(new Options()));
    }

    public FileIndexOptions(CoreOptions coreOptions) {
        this.indexTypeOptions = new HashMap<>();
        this.topLevelMapColumnOptions = new HashMap<>();
        this.fileIndexInManifestThreshold = coreOptions.fileIndexInManifestThreshold();
        setupOptions(coreOptions);
    }

    private void setupOptions(CoreOptions coreOptions) {
        String fileIndexPrefix = FILE_INDEX + ".";
        String fileIndexColumnSuffix = "." + COLUMNS;

        Map<String, String> optionMap = new HashMap<>();
        // find the column to be indexed.
        for (Map.Entry<String, String> entry : coreOptions.toMap().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(fileIndexPrefix)) {
                // start with file-index, decode this option
                if (key.endsWith(fileIndexColumnSuffix)) {
                    // if end with .column, set up indexes
                    String indexType =
                            key.substring(
                                    fileIndexPrefix.length(),
                                    key.length() - fileIndexColumnSuffix.length());
                    String[] names = entry.getValue().split(",");
                    for (String name : names) {
                        if (StringUtils.isBlank(name)) {
                            throw new IllegalArgumentException(
                                    "Wrong option in " + key + ", should not have empty column");
                        }
                        computeIfAbsent(name.trim(), indexType);
                    }
                } else {
                    optionMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // fill out the options
        for (Map.Entry<String, String> optionEntry : optionMap.entrySet()) {
            String key = optionEntry.getKey();

            String[] kv = key.substring(fileIndexPrefix.length()).split("\\.");
            if (kv.length != 3) {
                // just ignore options those are not expected
                continue;
            }
            String indexType = kv[0];
            String cname = kv[1];
            String opkey = kv[2];

            // if reaches here, must be an option.
            if (get(cname, indexType) != null) {
                get(cname, indexType).set(opkey, optionEntry.getValue());
            } else if (getMapTopLevelOptions(cname, indexType) != null) {
                getMapTopLevelOptions(cname, indexType).set(opkey, optionEntry.getValue());
            } else {
                throw new IllegalArgumentException(
                        "Wrong option in \""
                                + key
                                + "\", can't found column \""
                                + cname
                                + "\" in \""
                                + fileIndexPrefix
                                + indexType
                                + fileIndexColumnSuffix
                                + "\"");
            }
        }
    }

    private void computeIfAbsent(String column, String indexType) {
        Optional<Integer> nestedColumnPosition = topLevelIndexOfNested(column);
        if (nestedColumnPosition.isPresent()) {
            int position = nestedColumnPosition.get();
            String columnName = column.substring(0, position);
            String nestedName = column.substring(position + 1, column.length() - 1);

            indexTypeOptions
                    .computeIfAbsent(new Column(columnName, nestedName), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
            topLevelMapColumnOptions
                    .computeIfAbsent(new Column(columnName), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
        } else {
            indexTypeOptions
                    .computeIfAbsent(new Column(column), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
        }
    }

    private Options get(String column, String indexType) {
        Optional<Integer> nestedColumnPosition = topLevelIndexOfNested(column);

        Column columnKey;
        if (nestedColumnPosition.isPresent()) {
            int position = nestedColumnPosition.get();
            String columnName = column.substring(0, position);
            String nestedName = column.substring(position + 1, column.length() - 1);

            columnKey = new Column(columnName, nestedName);
        } else {
            columnKey = new Column(column);
        }

        return Optional.ofNullable(indexTypeOptions.getOrDefault(columnKey, null))
                .map(x -> x.get(indexType))
                .orElse(null);
    }

    public Options getMapTopLevelOptions(String column, String indexType) {
        return Optional.ofNullable(topLevelMapColumnOptions.getOrDefault(new Column(column), null))
                .map(x -> x.get(indexType))
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Can't find top level column options for map type: "
                                                + column
                                                + " "
                                                + indexType));
    }

    public boolean isEmpty() {
        return indexTypeOptions.isEmpty();
    }

    public long fileIndexInManifestThreshold() {
        return fileIndexInManifestThreshold;
    }

    public Set<Map.Entry<Column, Map<String, Options>>> entrySet() {
        return indexTypeOptions.entrySet();
    }

    public static Optional<Integer> topLevelIndexOfNested(String column) {
        int start = column.indexOf('[');
        if (start != -1 && column.endsWith("]")) {
            return Optional.of(start);
        }
        return Optional.empty();
    }

    /** Column to be file indexed. */
    public static class Column {

        private final String columnName;
        private final String nestedColumnName;
        private final boolean isNestedColumn;

        public Column(String columnName) {
            this.columnName = columnName;
            this.nestedColumnName = null;
            this.isNestedColumn = false;
        }

        public Column(String columnName, String nestedColumnName) {
            this.columnName = columnName;
            this.nestedColumnName = nestedColumnName;
            this.isNestedColumn = true;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getNestedColumnName() {
            if (!isNestedColumn) {
                throw new RuntimeException(
                        "Column " + columnName + " is not nested column in options.");
            }
            return nestedColumnName;
        }

        public boolean isNestedColumn() {
            return isNestedColumn;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[] {columnName, nestedColumnName, isNestedColumn});
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Column)) {
                return false;
            }

            Column that = (Column) obj;

            return Objects.equals(columnName, that.columnName)
                    && Objects.equals(nestedColumnName, that.nestedColumnName)
                    && isNestedColumn == that.isNestedColumn;
        }
    }
}
