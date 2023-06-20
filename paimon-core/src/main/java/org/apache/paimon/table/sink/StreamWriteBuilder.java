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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;

/**
 * An interface for building the {@link StreamTableWrite} and {@link StreamTableCommit}.
 *
 * <p>Key points to achieve exactly-once consistency:
 *
 * <ul>
 *   <li>CommitUser represents a user. A user can commit multiple times. In distributed processing,
 *       you are expected to use the same commitUser.
 *   <li>Different applications need to use different commitUsers.
 *   <li>The commitIdentifier of {@link StreamTableWrite} and {@link StreamTableCommit} needs to be
 *       consistent, and the id needs to be incremented for the next committing.
 *   <li>When a failure occurs, if you still have uncommitted {@link CommitMessage}s, please retry
 *       with {@link StreamTableCommit#filterAndCommit} to exclude the committed messages by
 *       commitIdentifier.
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface StreamWriteBuilder extends WriteBuilder {

    /** Get commit user, set by {@link #withCommitUser}. */
    String commitUser();

    /**
     * Set commit user, the default value is a random UUID. The commit user used by {@link
     * TableWrite} and {@link TableCommit} must be the same, otherwise there will be some conflicts.
     */
    StreamWriteBuilder withCommitUser(String commitUser);

    /** Create a {@link TableWrite} to write {@link InternalRow}s. */
    @Override
    StreamTableWrite newWrite();

    /** Create a {@link TableCommit} to commit {@link CommitMessage}s. */
    @Override
    StreamTableCommit newCommit();
}
