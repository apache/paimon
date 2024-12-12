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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

/** Tests for meta files in {@link AppendOnlyFileStoreTable} with schema evolution. */
public class AppendOnlyTableFileMetaFilterTest extends FileMetaFilterTestBase {

    @BeforeEach
    public void before() throws Exception {
        super.before();
    }

    @Override
    protected FileStoreTable createFileStoreTable(Map<Long, TableSchema> tableSchemas) {
        SchemaManager schemaManager = new TestingSchemaManager(tablePath, tableSchemas);
        return new AppendOnlyFileStoreTable(
                fileIO, tablePathProvider, schemaManager.latest().get()) {
            @Override
            public SchemaManager schemaManager() {
                return schemaManager;
            }
        };
    }

    @Override
    protected SimpleStats getTableValueStats(DataFileMeta fileMeta) {
        return fileMeta.valueStats();
    }
}
