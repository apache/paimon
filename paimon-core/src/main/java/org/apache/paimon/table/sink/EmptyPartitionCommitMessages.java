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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Utilities to add an empty data file for empty static partition overwrite. */
public class EmptyPartitionCommitMessages {

    private EmptyPartitionCommitMessages() {}

    public static List<CommitMessage> appendIfNeeded(
            FileStoreTable table,
            BatchWriteBuilder writeBuilder,
            @Nullable Map<String, String> overwritePartitionSpec,
            List<CommitMessage> commitMessages)
            throws Exception {
        if (!shouldWriteEmptyPartition(table, overwritePartitionSpec, commitMessages)) {
            return commitMessages;
        }

        List<CommitMessage> result =
                commitMessages == null ? new ArrayList<>() : new ArrayList<>(commitMessages);
        BinaryRow overwritePartition = convertSpecToBinaryRow(table, overwritePartitionSpec);
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            ((InnerTableWrite) write).writeEmptyFile(overwritePartition, 0);
            result.addAll(write.prepareCommit());
        }
        return result;
    }

    public static boolean shouldWriteEmptyPartition(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartitionSpec,
            List<CommitMessage> commitMessages) {
        return table.coreOptions().writeEmptyPartitionEnable()
                && isFullStaticOverwrite(table, overwritePartitionSpec)
                && !hasNewFiles(commitMessages);
    }

    public static boolean hasNewFiles(List<CommitMessage> commitMessages) {
        if (commitMessages == null || commitMessages.isEmpty()) {
            return false;
        }
        for (CommitMessage commitMessage : commitMessages) {
            if (commitMessage instanceof CommitMessageImpl) {
                DataIncrement dataIncrement =
                        ((CommitMessageImpl) commitMessage).newFilesIncrement();
                if (dataIncrement != null
                        && dataIncrement.newFiles() != null
                        && !dataIncrement.newFiles().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isFullStaticOverwrite(
            FileStoreTable table, @Nullable Map<String, String> overwritePartitionSpec) {
        return overwritePartitionSpec != null
                && !overwritePartitionSpec.isEmpty()
                && table.schema().partitionKeys().stream()
                        .allMatch(overwritePartitionSpec::containsKey);
    }

    private static BinaryRow convertSpecToBinaryRow(
            FileStoreTable table, Map<String, String> overwritePartitionSpec) {
        RowType partitionType = table.schema().logicalPartitionType();
        return InternalSerializers.create(partitionType)
                .toBinaryRow(
                        InternalRowPartitionComputer.convertSpecToInternalRow(
                                overwritePartitionSpec,
                                partitionType,
                                table.coreOptions().partitionDefaultName()))
                .copy();
    }
}
