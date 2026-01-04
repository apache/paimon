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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** Implementation for {@link WriteBuilder}. */
public class BatchWriteBuilderImpl implements BatchWriteBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;
    private final String commitUser;

    private Map<String, String> staticPartition;
    private boolean appendCommitCheckConflict = false;

    public BatchWriteBuilderImpl(InnerTable table) {
        this.table = table;
        this.commitUser = createCommitUser(new Options(table.options()));
    }

    private BatchWriteBuilderImpl(
            InnerTable table, String commitUser, @Nullable Map<String, String> staticPartition) {
        this.table = table;
        this.commitUser = commitUser;
        this.staticPartition = staticPartition;
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType rowType() {
        return table.rowType();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return table.newWriteSelector();
    }

    @Override
    public BatchWriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        this.staticPartition = staticPartition;
        return this;
    }

    @Override
    public BatchTableWrite newWrite() {
        return table.newWrite(commitUser).withIgnorePreviousFiles(staticPartition != null);
    }

    @Override
    public BatchTableCommit newCommit() {
        InnerTableCommit commit =
                table.newCommit(commitUser)
                        .withOverwrite(staticPartition)
                        .appendCommitCheckConflict(appendCommitCheckConflict);
        commit.ignoreEmptyCommit(
                Options.fromMap(table.options())
                        .getOptional(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT)
                        .orElse(true));
        return commit;
    }

    public BatchWriteBuilderImpl copyWithNewTable(Table newTable) {
        return new BatchWriteBuilderImpl((InnerTable) newTable, commitUser, staticPartition);
    }

    public BatchWriteBuilderImpl appendCommitCheckConflict(boolean appendCommitCheckConflict) {
        this.appendCommitCheckConflict = appendCommitCheckConflict;
        return this;
    }
}
