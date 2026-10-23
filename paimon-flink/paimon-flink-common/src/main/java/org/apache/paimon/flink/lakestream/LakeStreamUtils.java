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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.HashMap;
import java.util.Map;

/** Utilities for lake-stream mode. */
public class LakeStreamUtils {

    /** The {@code $rt} suffix, e.g. {@code my_table$rt}. */
    public static final String REAL_TIME_TABLE_SUFFIX = "rt";

    /**
     * Whether the identifier carries the {@code $rt} suffix. Branch access such as {@code
     * t1$branch_b1$rt} deliberately does not.
     */
    public static boolean isRealTimeTable(Identifier identifier) {
        return identifier.getBranchName() == null
                && REAL_TIME_TABLE_SUFFIX.equals(identifier.getSystemTableName());
    }

    public static boolean isRealTimeTable(ObjectPath tablePath) {
        return isRealTimeTable(FlinkCatalog.toIdentifier(tablePath));
    }

    /** Strips the {@code $rt} suffix, keeping catalog and database. */
    public static ObjectIdentifier stripRealTimeSuffix(ObjectIdentifier identifier) {
        Identifier parsed =
                new Identifier(identifier.getDatabaseName(), identifier.getObjectName());
        return ObjectIdentifier.of(
                identifier.getCatalogName(), identifier.getDatabaseName(), parsed.getTableName());
    }

    public static ObjectPath stripRealTimeSuffix(ObjectPath tablePath) {
        return new ObjectPath(
                tablePath.getDatabaseName(), FlinkCatalog.toIdentifier(tablePath).getTableName());
    }

    /**
     * Extracts every entry whose key starts with {@code prefix}, with the prefix removed, e.g.
     * {@code fluss.bootstrap.servers} becomes {@code bootstrap.servers} for the prefix {@code
     * fluss.}.
     */
    public static Map<String, String> extractPrefixedOptions(
            Map<String, String> options, String prefix) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }
}
