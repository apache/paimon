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
import org.apache.paimon.tag.TagTimeExpire;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

/** A procedure to expire tags by time. */
public class ExpireTagsProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "expire_tags";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "expiration_time",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public @DataTypeHint("ROW<expired_tags STRING>") Row[] call(
            ProcedureContext procedureContext, String tableId, @Nullable String expirationTimeStr)
            throws Catalog.TableNotExistException {
        TagTimeExpire tagTimeExpire = table(tableId).newExpireTags();
        if (expirationTimeStr != null) {
            LocalDateTime expirationTime =
                    DateTimeUtils.parseTimestampData(expirationTimeStr, 3, TimeZone.getDefault())
                            .toLocalDateTime();
            tagTimeExpire.withExpirationTime(expirationTime);
        }
        List<String> expired = tagTimeExpire.expire();
        return expired.isEmpty()
                ? new Row[] {Row.of("No expired tags.")}
                : expired.stream().map(x -> Row.of(x)).toArray(Row[]::new);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
