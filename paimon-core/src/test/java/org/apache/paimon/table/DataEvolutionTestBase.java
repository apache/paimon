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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** Base class for DataEvolution-related tests. */
public class DataEvolutionTestBase extends TableTestBase {

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    protected void write(long count) throws Exception {
        createTableDefault();

        Schema schema = schemaDefault();
        RowType writeType0 = schema.rowType().project(Arrays.asList("f0", "f1"));
        RowType writeType1 = schema.rowType().project(Collections.singletonList("f2"));
        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write0 = builder.newWrite().withWriteType(writeType0)) {
            for (int i = 0; i < count; i++) {
                write0.write(GenericRow.of(i, BinaryString.fromString("a" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            commit.commit(write0.prepareCommit());
        }

        long rowId = getTableDefault().snapshotManager().latestSnapshot().nextRowId() - count;
        builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write1 = builder.newWrite().withWriteType(writeType1)) {
            for (int i = 0; i < count; i++) {
                write1.write(GenericRow.of(BinaryString.fromString("b" + i)));
            }
            BatchTableCommit commit = builder.newCommit();
            List<CommitMessage> commitables = write1.prepareCommit();
            setFirstRowId(commitables, rowId);
            commit.commit(commitables);
        }
    }

    protected void setFirstRowId(List<CommitMessage> commitables, long firstRowId) {
        commitables.forEach(
                c -> {
                    CommitMessageImpl commitMessage = (CommitMessageImpl) c;
                    List<DataFileMeta> newFiles =
                            new ArrayList<>(commitMessage.newFilesIncrement().newFiles());
                    commitMessage.newFilesIncrement().newFiles().clear();
                    commitMessage
                            .newFilesIncrement()
                            .newFiles()
                            .addAll(
                                    newFiles.stream()
                                            .map(s -> s.assignFirstRowId(firstRowId))
                                            .collect(Collectors.toList()));
                });
    }
}
