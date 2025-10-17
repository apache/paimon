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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.TableType;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.ChainFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;

import javax.annotation.Nullable;

import java.util.Map;

/** Utils for table. */
public class ChainTableUtils {

    public static void checkChainTableOptions(
            Map<String, String> tableOptions, @Nullable String primaryKeys, boolean partitionTbl) {
        if (Boolean.parseBoolean(tableOptions.get(CoreOptions.CHAIN_TABLE_ENABLED.key()))) {
            Options options = Options.fromMap(tableOptions);
            TableType tableType = options.get(CoreOptions.TYPE);
            primaryKeys = primaryKeys == null ? options.get(CoreOptions.PRIMARY_KEY) : primaryKeys;
            String sequenceField = options.get(CoreOptions.SEQUENCE_FIELD);
            int bucket = options.get(CoreOptions.BUCKET);
            MergeEngine mergeEngine = options.get(CoreOptions.MERGE_ENGINE);
            Preconditions.checkArgument(
                    tableType == TableType.TABLE, "Chain table must be table type.");
            Preconditions.checkArgument(
                    primaryKeys != null, "Primary key is required for chain table.");
            Preconditions.checkArgument(
                    sequenceField != null, "Sequence field is required for chain table.");
            Preconditions.checkArgument(bucket > 0, "Bucket number must be greater than 0.");
            Preconditions.checkArgument(
                    mergeEngine == MergeEngine.DEDUPLICATE, "Merge engine must be deduplicate.");
            Preconditions.checkArgument(partitionTbl, "Chain table must be partition table.");
        }
    }

    public static FileStoreTable getWriteTable(FileStoreTable fileStoreTable) {
        if (fileStoreTable.coreOptions().isChainTable()) {
            FileStoreTable candidateFileStoreTable =
                    fileStoreTable instanceof FallbackReadFileStoreTable
                            ? ((FallbackReadFileStoreTable) fileStoreTable).primaryTable()
                            : fileStoreTable;
            Preconditions.checkArgument(
                    candidateFileStoreTable instanceof PrimaryKeyFileStoreTable,
                    "Chain table must be primary key table.");
            return candidateFileStoreTable;
        } else {
            return fileStoreTable;
        }
    }

    public static Table getReadTable(Table table) {
        CoreOptions options =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).coreOptions()
                        : CoreOptions.fromMap(table.options());
        if (options.isScanFallbackChainBranch()) {
            if (table instanceof FallbackReadFileStoreTable) {
                if (isChainScanFallbackSnapshotBranch(options)) {
                    return ((ChainFileStoreTable) (((FallbackReadFileStoreTable) table).fallback()))
                            .primaryTable();
                } else {
                    return ((ChainFileStoreTable) (((FallbackReadFileStoreTable) table).fallback()))
                            .fallback();
                }
            }
            return table;
        }
        return table;
    }

    public static boolean isChainScanFallbackSnapshotBranch(CoreOptions options) {
        String currentBranch = options.branch();
        return options.isChainTable()
                && currentBranch.equalsIgnoreCase(options.scanFallbackSnapshotBranch());
    }

    public static boolean isChainScanFallbackDeltaBranch(CoreOptions options) {
        String currentBranch = options.branch();
        return options.isChainTable()
                && currentBranch.equalsIgnoreCase(options.scanFallbackDeltaBranch());
    }

    public static boolean isScanFallbackChainRead(Map<String, String> tableOptions) {
        CoreOptions options = CoreOptions.fromMap(tableOptions);
        return options.isScanFallbackChainRead();
    }
}
