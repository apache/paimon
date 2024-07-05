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
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * Create tag procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.create_tag('tableId', 'tagName', snapshotId, 'timeRetained')
 * </code></pre>
 */
public class CreateTagProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "create_tag";

    public String[] call(
            ProcedureContext procedureContext, String tableId, String tagName, long snapshotId)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, tagName, snapshotId, null);
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String tagName)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, tagName, null, null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String tagName,
            long snapshotId,
            String timeRetained)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, tagName, snapshotId, timeRetained);
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String tagName, String timeRetained)
            throws Catalog.TableNotExistException {
        return innerCall(tableId, tagName, null, timeRetained);
    }

    private String[] innerCall(
            String tableId,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained)
            throws Catalog.TableNotExistException {
        Table table = catalog.getTable(Identifier.fromString(tableId));
        if (snapshotId == null) {
            table.createTag(tagName, toDuration(timeRetained));
        } else {
            table.createTag(tagName, snapshotId, toDuration(timeRetained));
        }
        return new String[] {"Success"};
    }

    @Nullable
    private static Duration toDuration(@Nullable String s) {
        if (s == null) {
            return null;
        }

        return TimeUtils.parseDuration(s);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
