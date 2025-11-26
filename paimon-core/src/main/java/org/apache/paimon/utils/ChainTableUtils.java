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
import org.apache.paimon.CoreOptions.ChainBranchReadMode;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.ChainFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PrimaryKeyFileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Map;

/** Utils for table. */
public class ChainTableUtils {

    public static boolean isChainTable(Table table) {
        return table != null
                && table.options() != null
                && Options.fromMap(table.options()).get(CoreOptions.CHAIN_TABLE_ENABLED);
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

    public static Table getTable(Table table) {
        CoreOptions options =
                table instanceof FileStoreTable
                        ? ((FileStoreTable) table).coreOptions()
                        : CoreOptions.fromMap(table.options());
        if (isScanFallbackChainBranch(options) && table instanceof FallbackReadFileStoreTable) {
            FileStoreTable fileStoreTable;
            ChainFileStoreTable chainFileStoreTable =
                    (ChainFileStoreTable) (((FallbackReadFileStoreTable) table).fallback());
            if (isChainScanFallbackSnapshotBranch(options)) {
                fileStoreTable = chainFileStoreTable.primaryTable();
            } else {
                fileStoreTable = chainFileStoreTable.fallback();
            }
            fileStoreTable
                    .schema()
                    .options()
                    .put(
                            CoreOptions.CHAIN_TABLE_BRANCH_INTERNAL_READ_MODE.key(),
                            ChainBranchReadMode.DEFAULT.name());
            return fileStoreTable;
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

    public static boolean isScanFallbackChainBranch(CoreOptions options) {
        String branch = options.branch();
        return options.isChainTable()
                && (branch.equalsIgnoreCase(options.scanFallbackSnapshotBranch())
                        || branch.equalsIgnoreCase(options.scanFallbackDeltaBranch()));
    }

    public static boolean isScanFallbackChainRead(Map<String, String> tableOptions) {
        CoreOptions options = CoreOptions.fromMap(tableOptions);
        return isScanFallbackChainRead(options);
    }

    public static boolean isScanFallbackChainRead(CoreOptions options) {
        return isScanFallbackChainBranch(options)
                && ChainBranchReadMode.CHAIN_READ == options.getChainBranchReadMode();
    }
}
