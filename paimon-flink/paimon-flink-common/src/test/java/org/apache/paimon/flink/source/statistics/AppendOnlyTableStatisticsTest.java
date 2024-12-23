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

package org.apache.paimon.flink.source.statistics;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

/** Statistics tests for append only tables. */
public class AppendOnlyTableStatisticsTest extends FileStoreTableStatisticsTestBase {
    @Override
    protected FileStoreTable createStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.TABLE_SCHEMA_PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, 1);
        TableSchema tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath)
                        .createTable(schemaBuilder().partitionKeys("pt").build());
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }
}
