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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An abstraction layer above {@link FileStore} to provide reading and writing of {@link
 * InternalRow}.
 */
public interface FileStoreTable extends DataTable {

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

    @Override
    default List<String> primaryKeys() {
        return schema().primaryKeys();
    }

    @Override
    default Map<String, String> options() {
        return schema().options();
    }

    @Override
    default Optional<String> comment() {
        return Optional.ofNullable(schema().comment());
    }

    TableSchema schema();

    FileStore<?> store();

    BucketMode bucketMode();

    CatalogEnvironment catalogEnvironment();

    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    FileStoreTable copy(TableSchema newTableSchema);

    /** Doesn't change table schema even when there exists time travel scan options. */
    FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions);

    /** TODO: this method is weird, old options will overwrite new options. */
    FileStoreTable copyWithLatestSchema();

    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    TableWriteImpl<?> newWrite(String commitUser, ManifestCacheFilter manifestFilter);

    @Override
    TableCommitImpl newCommit(String commitUser);

    TableCommitImpl newCommit(String commitUser, String branchName);

    LocalTableQuery newLocalTableQuery();

    default SimpleStats getSchemaFieldStats(DataFileMeta dataFileMeta) {
        return dataFileMeta.valueStats();
    }

    boolean supportStreamingReadOverwrite();

    RowKeyExtractor createRowKeyExtractor();
}
