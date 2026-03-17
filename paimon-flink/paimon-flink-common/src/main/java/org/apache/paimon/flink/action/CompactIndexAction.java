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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.compact.CompactIndexTableCompact;
import org.apache.paimon.table.FileStoreTable;

import java.util.Map;

/** Flink action for distributed compaction of BTree global index files. */
public class CompactIndexAction extends TableActionBase {

    private final Map<String, String> tableConf;

    public CompactIndexAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig);
        this.tableConf = tableConf;
        this.forceStartFlinkJob = true;
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact_index action. "
                                    + "The table type is '%s'.",
                            table.getClass().getName()));
        }
        if (!tableConf.isEmpty()) {
            table = table.copy(tableConf);
        }
    }

    @Override
    public void build() throws Exception {
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        CompactIndexTableCompact builder =
                new CompactIndexTableCompact(env, identifier.getFullName(), fileStoreTable);
        builder.build();
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Compact Index job : " + identifier.getFullName());
    }
}
