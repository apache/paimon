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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.table.procedure.ProcedureContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Delete tag by timestamp procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.delete_tags_timestamp('tableId', 1736932554000)
 * </code></pre>
 */
public class DeleteTagsByTimestampProcedure extends ProcedureBase {
    public static final String IDENTIFIER = "delete_tags_timestamp";

    public String[] call(ProcedureContext procedureContext, String tableId, Long timestamp)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        TagManager tagManager = fileStoreTable.tagManager();
        FileIO fileIO = fileStoreTable.fileIO();
        List<String> deleteTags = new ArrayList<>();
        try {
            for (String tagName : fileStoreTable.tagManager().allTagNames()) {
                FileStatus fileStatus = fileIO.listStatus(tagManager.tagPath(tagName))[0];
                if (fileStatus.getModificationTime() < timestamp) {
                    deleteTags.add(tagName);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (deleteTags.size() > 0) {
            table.deleteTags(String.join(",", deleteTags));
        }

        return new String[] {"Success"};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
