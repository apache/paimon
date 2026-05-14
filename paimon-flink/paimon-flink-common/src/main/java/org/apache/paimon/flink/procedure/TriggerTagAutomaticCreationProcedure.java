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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.tag.TagPeriodHandler;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.Collections;

/**
 * A procedure to trigger tag automatic creation for a table. Usage:
 *
 * <pre><code>
 *  -- create an auto tag if this is a tag automatic creation table.
 *  CALL sys.trigger_tag_automatic_creation(`table` => 'tableId')
 * </code></pre>
 */
public class TriggerTagAutomaticCreationProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "trigger_tag_automatic_creation";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "force", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public @DataTypeHint("ROW<result STRING>") Row[] call(
            ProcedureContext procedureContext, String tableId, Boolean force) throws Exception {
        FileStoreTable fsTable =
                ((FileStoreTable) catalog.getTable(Identifier.fromString(tableId)));
        String commitUser = CoreOptions.fromMap(fsTable.options()).createCommitUser();

        try (TableCommitImpl commit = fsTable.newCommit(commitUser)) {
            TagAutoManager tam = fsTable.newTagAutoManager();
            if (tam.getTagAutoCreation() != null) {
                // Fist try
                tam.run();

                // If the expected tag not created, try again with an empty commit
                if (force
                        && !isAutoTagExits(fsTable)
                        && tam.getTagAutoCreation().forceCreatingSnapshot()) {
                    ManifestCommittable committable =
                            new ManifestCommittable(Long.MAX_VALUE, null, Collections.emptyList());
                    commit.ignoreEmptyCommit(false);
                    commit.commit(committable);
                    // Second try
                    tam.run();
                }
            }
        }

        return new Row[] {Row.of("Success")};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private boolean isAutoTagExits(FileStoreTable table) {
        TagPeriodHandler periodHandler =
                TagPeriodHandler.create(CoreOptions.fromMap(table.options()));

        // With forceCreatingSnapshot, the auto-tag should exist for "now"
        LocalDateTime tagTime = periodHandler.normalizeToPreviousTag(LocalDateTime.now());
        String tagName = periodHandler.timeToTag(tagTime);
        return table.tagManager().tagExists(tagName);
    }
}
