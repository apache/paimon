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

package org.apache.flink.table.store.table;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.CatalogOptions;
import org.apache.flink.table.store.options.Options;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.flink.table.store.CoreOptions.PATH;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

    public static FileStoreTable create(CatalogOptions options) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(CoreOptions.path(options.options()), options);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return create(fileIO, options.options());
    }

    public static FileStoreTable create(FileIO fileIO, Path path) {
        Options options = new Options();
        options.set(PATH, path.toString());
        return create(fileIO, options);
    }

    public static FileStoreTable create(FileIO fileIO, Options options) {
        Path tablePath = CoreOptions.path(options);
        TableSchema tableSchema =
                new SchemaManager(fileIO, tablePath)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));
        return create(fileIO, tablePath, tableSchema, options);
    }

    public static FileStoreTable create(FileIO fileIO, Path tablePath, TableSchema tableSchema) {
        return create(fileIO, tablePath, tableSchema, new Options());
    }

    public static FileStoreTable create(
            FileIO fileIO, Path tablePath, TableSchema tableSchema, Options dynamicOptions) {
        FileStoreTable table;
        if (Options.fromMap(tableSchema.options()).get(CoreOptions.WRITE_MODE)
                == WriteMode.APPEND_ONLY) {
            table = new AppendOnlyFileStoreTable(fileIO, tablePath, tableSchema);
        } else {
            if (tableSchema.primaryKeys().isEmpty()) {
                table = new ChangelogValueCountFileStoreTable(fileIO, tablePath, tableSchema);
            } else {
                table = new ChangelogWithKeyFileStoreTable(fileIO, tablePath, tableSchema);
            }
        }

        return table.copy(dynamicOptions.toMap());
    }
}
