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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/** Used to filter tables in whole database synchronization. */
public class DatabaseSyncTableFilter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CdcActionCommonUtils.class);

    private static final long serialVersionUID = 1L;
    private final Pattern includingPattern;
    private final Pattern excludingPattern;

    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();

    public DatabaseSyncTableFilter(String includingTables, @Nullable String excludingTables) {
        this.includingPattern = Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
    }

    public boolean filter(String currentDataBase, String currentTable, JsonNode record) {
        // database synchronization needs this, so we validate the record here
        if (currentDataBase == null || currentTable == null) {
            throw new IllegalArgumentException(
                    "Cannot synchronize record when database name or table name is unknown. "
                            + "Invalid record is:\n"
                            + record);
        }

        // In case the record is incomplete, we let the null value pass validation
        // and handle the null value when we really need it
        if (currentTable == null) {
            return true;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        boolean shouldSynchronize = true;
        if (includingPattern != null) {
            shouldSynchronize = includingPattern.matcher(currentTable).matches();
        }
        if (excludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !excludingPattern.matcher(currentTable).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source table {} won't be synchronized because it was excluded. ",
                    currentTable);
            excludedTables.add(currentTable);
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }
}
