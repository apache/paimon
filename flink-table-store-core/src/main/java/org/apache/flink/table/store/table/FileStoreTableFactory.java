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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.CoreOptions.LogStartupMode;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;

import static org.apache.flink.table.store.CoreOptions.PATH;

/** Factory to create {@link FileStoreTable}. */
public class FileStoreTableFactory {

    public static FileStoreTable create(Path path) {
        Configuration conf = new Configuration();
        conf.set(PATH, path.toString());
        return create(conf);
    }

    public static FileStoreTable create(Configuration conf) {
        Path tablePath = CoreOptions.path(conf);
        TableSchema tableSchema =
                new SchemaManager(tablePath)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));
        return create(tablePath, tableSchema, conf);
    }

    public static FileStoreTable create(Path tablePath, TableSchema tableSchema) {
        return create(tablePath, tableSchema, new Configuration());
    }

    public static FileStoreTable create(
            Path tablePath, TableSchema tableSchema, Configuration dynamicOptions) {
        // merge dynamic options into schema.options
        Configuration newOptions = Configuration.fromMap(tableSchema.options());
        dynamicOptions.toMap().forEach(newOptions::setString);
        newOptions.set(PATH, tablePath.toString());

        // validate merged options
        validateOptions(new CoreOptions(newOptions));

        // copy a new table store to contain dynamic options
        tableSchema = tableSchema.copy(newOptions.toMap());

        SchemaManager schemaManager = new SchemaManager(tablePath);
        if (newOptions.get(CoreOptions.WRITE_MODE) == WriteMode.APPEND_ONLY) {
            return new AppendOnlyFileStoreTable(tablePath, schemaManager, tableSchema);
        } else {
            if (tableSchema.primaryKeys().isEmpty()) {
                return new ChangelogValueCountFileStoreTable(tablePath, schemaManager, tableSchema);
            } else {
                return new ChangelogWithKeyFileStoreTable(tablePath, schemaManager, tableSchema);
            }
        }
    }

    private static void validateOptions(CoreOptions options) {
        if (options.logStartupMode() == LogStartupMode.FROM_TIMESTAMP) {
            if (options.logScanTimestampMills() == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.LOG_SCAN_TIMESTAMP_MILLS.key(),
                                CoreOptions.LogStartupMode.FROM_TIMESTAMP,
                                CoreOptions.LOG_SCAN.key()));
            }
        }
    }
}
