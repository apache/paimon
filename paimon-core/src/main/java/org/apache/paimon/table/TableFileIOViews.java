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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FilePrefixHistory;
import org.apache.paimon.fs.IoCacheViews;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TableFileIOView;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.CoreOptions.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.CoreOptions.DATA_FILE_PREFIX;

/** Builds the {@link TableFileIOView} every {@link FileStoreTable} applies to its FileIO. */
final class TableFileIOViews {

    private TableFileIOViews() {}

    /** The FileIO of the table at {@code path} with {@code options}. */
    static FileIO forTable(FileIO fileIO, Path path, Map<String, String> options) {
        List<String> prefixes = new ArrayList<>(2);
        prefixes.add(options.get(DATA_FILE_PREFIX.key()));
        prefixes.add(options.get(CHANGELOG_FILE_PREFIX.key()));
        // only files under the table's location use io-cache, not those under external paths
        return IoCacheViews.withTableView(
                fileIO,
                TableFileIOView.of(
                        Collections.singletonList(path),
                        prefixes,
                        new SchemaFilePrefixHistory(path, CoreOptions.branch(options))));
    }

    /** The data and changelog file prefixes of every schema of a branch, read from storage. */
    static final class SchemaFilePrefixHistory implements FilePrefixHistory {

        private static final long serialVersionUID = 1L;

        private final Path path;
        private final String branch;

        SchemaFilePrefixHistory(Path path, String branch) {
            this.path = path;
            this.branch = branch;
        }

        @Override
        public List<String> filePrefixes(FileIO fileIO) {
            List<String> prefixes = new ArrayList<>();
            for (TableSchema schema : new FileSystemSchemaManager(fileIO, path, branch).listAll()) {
                for (String key :
                        new String[] {DATA_FILE_PREFIX.key(), CHANGELOG_FILE_PREFIX.key()}) {
                    String prefix = schema.options().get(key);
                    if (!StringUtils.isNullOrWhitespaceOnly(prefix) && !prefixes.contains(prefix)) {
                        prefixes.add(prefix);
                    }
                }
            }
            return prefixes;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof SchemaFilePrefixHistory)) {
                return false;
            }
            SchemaFilePrefixHistory that = (SchemaFilePrefixHistory) o;
            return path.equals(that.path) && branch.equals(that.branch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, branch);
        }
    }
}
