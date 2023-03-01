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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An interface for building the {@link TableWrite} and {@link TableCommit}.
 *
 * <p>Example of distributed batch writing:
 *
 * <pre>{@code
 * // 1. Create a WriteBuilder (Serializable)
 * Table table = catalog.getTable(...);
 * WriteBuilder builder = table.newWriteBuilder();
 *
 * // 2. Write records in distributed tasks
 * TableWrite write = builder.newWrite();
 * write.write(...);
 * write.write(...);
 * write.write(...);
 * List<CommitMessage> messages = write.prepareCommit(true, 0);
 *
 * // 3. Collect all CommitMessages to a global node and commit
 * TableCommit commit = builder.newCommit();
 * // commit transaction ID starts with zero, if you have further commits, please increment it.
 * commit.commit(0, allCommitMessages());
 * }</pre>
 *
 * @since 0.4.0
 */
@Experimental
public interface WriteBuilder extends Serializable {

    /** A name to identify this table. */
    String tableName();

    /** Returns the row type of this table. */
    RowType rowType();

    /** Get commit user, set by {@link #withCommitUser}. */
    String commitUser();

    /**
     * Set commit user, the default value is a random UUID. The commit user used by {@link
     * TableWrite} and {@link TableCommit} must be the same, otherwise there will be some conflicts.
     */
    WriteBuilder withCommitUser(String commitUser);

    /** Overwrite writing, same as the 'INSERT OVERWRITE' semantics of SQL. */
    default WriteBuilder withOverwrite() {
        withOverwrite(Collections.emptyMap());
        return this;
    }

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    default WriteBuilder withOverwrite(@Nullable Map<String, String> staticPartition) {
        if (staticPartition != null) {
            withOverwrite(Collections.singletonList(staticPartition));
        }
        return this;
    }

    /** Overwrite writing, multiple static partitions can be specified. */
    WriteBuilder withOverwrite(@Nullable List<Map<String, String>> staticPartitions);

    /** Create a {@link TableWrite} to write {@link InternalRow}s. */
    TableWrite newWrite();

    /** Create a {@link TableCommit} to commit {@link CommitMessage}s. */
    TableCommit newCommit();
}
