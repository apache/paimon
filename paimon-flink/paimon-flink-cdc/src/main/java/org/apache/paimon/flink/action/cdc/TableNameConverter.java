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

import org.apache.paimon.catalog.Identifier;

import java.io.Serializable;

/** Used to convert a MySQL source table name to corresponding Paimon table name. */
public class TableNameConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean caseSensitive;
    private final boolean mergeShards;
    private final String prefix;
    private final String suffix;

    public TableNameConverter(boolean caseSensitive) {
        this(caseSensitive, true, "", "");
    }

    public TableNameConverter(
            boolean caseSensitive, boolean mergeShards, String prefix, String suffix) {
        this.caseSensitive = caseSensitive;
        this.mergeShards = mergeShards;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public String convert(String originName) {
        String tableName = caseSensitive ? originName : originName.toLowerCase();
        return prefix + tableName + suffix;
    }

    public String convert(Identifier originIdentifier) {
        String rawName =
                mergeShards
                        ? originIdentifier.getObjectName()
                        : originIdentifier.getDatabaseName()
                                + "_"
                                + originIdentifier.getObjectName();
        return convert(rawName);
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }
}
