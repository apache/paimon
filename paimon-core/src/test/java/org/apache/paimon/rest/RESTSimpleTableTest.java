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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SimpleTableTestBase;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;

/** Base simple table test in rest catalog. */
public abstract class RESTSimpleTableTest extends SimpleTableTestBase {

    protected RESTCatalog restCatalog;
    protected String dataPath;

    @BeforeEach
    public void before() throws Exception {
        super.before();
        dataPath = tablePath.toString();
        restCatalog = createRESTCatalog();
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
    }

    protected abstract RESTCatalog createRESTCatalog() throws IOException;

    @AfterEach
    public void after() throws Exception {
        super.after();
        restCatalog.dropTable(identifier, true);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Options> configure, RowType rowType)
            throws Exception {
        Options conf = new Options();
        configure.accept(conf);
        if (!conf.contains(BUCKET_KEY) && conf.get(BUCKET) != -1) {
            conf.set(BUCKET_KEY, "a");
        }
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        if (restCatalog
                .listTables(identifier.getDatabaseName())
                .contains(identifier.getTableName())) {
            List<SchemaChange> schemaChangeList = new ArrayList<>();
            for (Map.Entry<String, String> entry : conf.toMap().entrySet()) {
                schemaChangeList.add(SchemaChange.setOption(entry.getKey(), entry.getValue()));
            }
            restCatalog.alterTable(identifier, schemaChangeList, true);
        } else {
            restCatalog.createTable(identifier, tableSchema.toSchema(), false);
        }
        return (FileStoreTable) restCatalog.getTable(identifier);
    }

    @Override
    protected FileStoreTable createBranchTable(String branch) throws Exception {
        if (!restCatalog.listBranches(identifier).contains(branch)) {
            restCatalog.createBranch(identifier, branch, null);
        }
        return (FileStoreTable)
                restCatalog.getTable(
                        new Identifier(
                                identifier.getDatabaseName(), identifier.getTableName(), branch));
    }

    @Test
    @Disabled("REST catalog does not support branchesCreatedFromTag yet")
    @Override
    public void testDeleteTagReferencedByBranch() {}
}
