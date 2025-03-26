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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.table.procedure.ProcedureContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/** A procedure to purge files for a table. */
public class PurgeFilesProcedure extends ProcedureBase {
    public static final String IDENTIFIER = "purge_files";

    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        return call(procedureContext, tableId, false);
    }

    public String[] call(ProcedureContext procedureContext, String tableId, boolean dryRun)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        FileIO fileIO = fileStoreTable.fileIO();
        Path tablePath = fileStoreTable.snapshotManager().tablePath();
        ArrayList<String> deleteDir;
        try {
            FileStatus[] fileStatuses = fileIO.listStatus(tablePath);
            deleteDir = new ArrayList<>(fileStatuses.length);
            Arrays.stream(fileStatuses)
                    .filter(f -> !f.getPath().getName().contains("schema"))
                    .forEach(
                            fileStatus -> {
                                try {
                                    deleteDir.add(fileStatus.getPath().getName());
                                    if (!dryRun) {
                                        fileIO.delete(fileStatus.getPath(), true);
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return deleteDir.isEmpty()
                ? new String[] {"There are no dir to be deleted."}
                : deleteDir.toArray(new String[0]);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
