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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Options of file index column. */
public class FileIndexOptions {

    public static final Pattern IS_NESTED = Pattern.compile(".+\\[.+]");

    public static final String FILE_INDEX = "file-index";

    public static final String COLUMNS = "columns";

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long fileIndexInManifestThreshold;

    private final Map<Column, Map<String, Options>> indexTypeOptions;

    public FileIndexOptions() {
        this(new CoreOptions(new Options()));
    }

    public FileIndexOptions(CoreOptions coreOptions) {
        this.indexTypeOptions = new HashMap<>();
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
            if (get(cname, indexType) == null) {
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
            get(cname, indexType).set(opkey, optionEntry.getValue());
        }
    }

    private void computeIfAbsent(String column, String indexType) {
        Optional<Integer> nestedColumnPosition = getNestedColumn(column);
        if (nestedColumnPosition.isPresent()) {
            int position = nestedColumnPosition.get();
            String columnName = column.substring(0, position);
            String nestedName = column.substring(position + 1, column.length() - 1);

            indexTypeOptions
                    .computeIfAbsent(new Column(columnName, nestedName), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
            indexTypeOptions
                    .computeIfAbsent(new Column(columnName, false), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
        } else {
            indexTypeOptions
                    .computeIfAbsent(new Column(column), c -> new HashMap<>())
                    .computeIfAbsent(indexType, i -> new Options());
        }
    }

    public Options get(String column, String indexType) {
        Optional<Integer> nestedColumnPosition = getNestedColumn(column);

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

    public boolean isEmpty() {
        return indexTypeOptions.isEmpty();
    }

    public long fileIndexInManifestThreshold() {
        return fileIndexInManifestThreshold;
    }

    public Set<Map.Entry<Column, Map<String, Options>>> entrySet() {
        return indexTypeOptions.entrySet().stream()
                .filter(entry -> entry.getKey().isExternallyPerceptible())
                .collect(Collectors.toSet());
    }

    public static Optional<Integer> getNestedColumn(String column) {
        if (IS_NESTED.matcher(column).find()) {
            return Optional.of(column.indexOf('['));
        }

        return Optional.empty();
    }

    /** Column to be file indexed. */
    public static class Column {

        private final String columnName;
        private final String nestedColumnName;
        private final boolean isNestedColumn;
        private final boolean externallyPerceptible;

        public Column(String columnName, boolean externallyPerceptible) {
            this.columnName = columnName;
            this.nestedColumnName = null;
            this.isNestedColumn = false;
            this.externallyPerceptible = externallyPerceptible;
        }

        public Column(String columnName) {
            this(columnName, true);
        }

        public Column(String columnName, String nestedColumnName) {
            this.columnName = columnName;
            this.nestedColumnName = nestedColumnName;
            this.isNestedColumn = true;
            this.externallyPerceptible = true;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isExternallyPerceptible() {
            return externallyPerceptible;
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
