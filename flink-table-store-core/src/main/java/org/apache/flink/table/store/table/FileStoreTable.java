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

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.table.sink.TableCommitImpl;
import org.apache.flink.table.store.table.sink.TableWriteImpl;
import org.apache.flink.table.store.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * An abstraction layer above {@link org.apache.flink.table.store.file.FileStore} to provide reading
 * and writing of {@link InternalRow}.
 */
public interface FileStoreTable extends DataTable, SupportsPartition {

    @Override
    default String name() {
        return location().getName();
    }

    @Override
    default RowType rowType() {
        return schema().logicalRowType();
    }

    @Override
    default List<String> partitionKeys() {
        return schema().partitionKeys();
    }

    TableSchema schema();

    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    FileStoreTable copyWithLatestSchema();

    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    @Override
    TableCommitImpl newCommit(String commitUser);

    default BinaryTableStats getSchemaFieldStats(DataFileMeta dataFileMeta) {
        return dataFileMeta.valueStats();
    }
}
