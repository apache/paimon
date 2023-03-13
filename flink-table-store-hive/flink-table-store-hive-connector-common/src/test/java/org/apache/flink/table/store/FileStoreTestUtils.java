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

package org.apache.flink.table.store;

import org.apache.flink.table.store.file.schema.Schema;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.types.RowType;

import java.util.List;

import static org.apache.flink.table.store.CoreOptions.PATH;

/** Test utils related to {@link org.apache.flink.table.store.file.FileStore}. */
public class FileStoreTestUtils {

    public static FileStoreTable createFileStoreTable(
            Options conf, RowType rowType, List<String> partitionKeys, List<String> primaryKeys)
            throws Exception {
        Path tablePath = CoreOptions.path(conf);
        new SchemaManager(LocalFileIO.create(), tablePath)
                .createTable(
                        new Schema(
                                rowType.getFields(), partitionKeys, primaryKeys, conf.toMap(), ""));

        // only path, other config should be read from file store.
        conf = new Options();
        conf.set(PATH, tablePath.toString());
        return FileStoreTableFactory.create(LocalFileIO.create(), conf);
    }
}
