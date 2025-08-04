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
import org.apache.paimon.FormatTableFileStore;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;

/** A {@link FileStoreTable} implementation for {@link FormatTableFileStore}. */
public class FormatTableFileStoreTable extends AppendOnlyFileStoreTable {

    private transient FormatTableFileStore lazyStore;

    public FormatTableFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    FormatTableFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        super(fileIO, path, tableSchema);
    }

    @Override
    public FormatTableFileStore store() {
        if (lazyStore == null) {
            lazyStore =
                    new FormatTableFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema,
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            tableSchema.logicalRowType().notNull(),
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }
}
