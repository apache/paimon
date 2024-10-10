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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.TagTimeExpire;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.table.procedure.ProcedureContext;

import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

/** A procedure to expire tags by time. */
public class ExpireTagsProcedure extends ProcedureBase {

    private static final String IDENTIFIER = "expire_tags";

    public String[] call(ProcedureContext procedureContext, String tableId)
            throws Catalog.TableNotExistException {
        return call(procedureContext, tableId, null);
    }

    public String[] call(ProcedureContext procedureContext, String tableId, String olderThanStr)
            throws Catalog.TableNotExistException {
        FileStoreTable fileStoreTable = (FileStoreTable) table(tableId);
        TagTimeExpire tagTimeExpire =
                fileStoreTable.store().newTagCreationManager().getTagTimeExpire();
        if (olderThanStr != null) {
            LocalDateTime olderThanTime =
                    DateTimeUtils.parseTimestampData(olderThanStr, 3, TimeZone.getDefault())
                            .toLocalDateTime();
            tagTimeExpire.withOlderThanTime(olderThanTime);
        }
        List<String> expired = tagTimeExpire.expire();
        return expired.isEmpty()
                ? new String[] {"No expired tags."}
                : expired.toArray(new String[0]);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
