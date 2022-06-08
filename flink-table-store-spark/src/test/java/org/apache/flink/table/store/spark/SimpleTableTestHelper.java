/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStore;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.operation.FileStoreCommit;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.writer.RecordWriter;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.apache.flink.table.data.binary.BinaryRowDataUtil.EMPTY_ROW;

/** TODO use new sink api in {@link FileStoreTable}. */
public class SimpleTableTestHelper {

    private final FileStore store;
    private final RecordWriter writer;

    public SimpleTableTestHelper(Path path, RowType rowType) throws Exception {
        Map<String, String> options = new HashMap<>();
        // orc is shaded, can not find shaded classes in ide
        options.put(FileStoreOptions.FILE_FORMAT.key(), "avro");
        new SchemaManager(path)
                .commitNewVersion(
                        new UpdateSchema(
                                rowType,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options,
                                ""));
        Configuration conf = Configuration.fromMap(options);
        conf.setString("path", path.toString());
        FileStoreTable table = FileStoreTableFactory.create(conf, "user");
        this.store = table.fileStore();
        this.writer =
                store.newWrite().createWriter(EMPTY_ROW, 0, Executors.newSingleThreadExecutor());
    }

    public void write(ValueKind kind, RowData record) throws Exception {
        long value = kind == ValueKind.ADD ? 1 : -1;
        writer.write(ValueKind.ADD, record, GenericRowData.of(value));
    }

    public void commit() throws Exception {
        ManifestCommittable committable = new ManifestCommittable(UUID.randomUUID().toString());
        writer.sync();
        Increment increment = writer.prepareCommit();
        committable.addFileCommittable(EMPTY_ROW, 0, increment);
        FileStoreCommit commit = store.newCommit();
        commit.commit(committable, Collections.emptyMap());
    }
}
