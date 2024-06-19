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

import java.util.List;

/**
 * A {@link TableWrite} for stream processing. You can use this class to commit multiple times.
 *
 * @since 0.4.0
 * @see StreamWriteBuilder
 */
@Public
public interface StreamTableWrite extends TableWrite {

    /**
     * Prepare commit for {@link TableCommit}. Collect incremental files for this write.
     *
     * @param waitCompaction whether to wait for the end of the background compaction.
     * @param commitIdentifier Committed transaction ID, can start from 0. If there are multiple
     *     commits, please increment this ID.
     * @see StreamTableCommit#commit
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;

    default boolean isStreamTableWrite() {
        return true;
    }

    default StreamTableWrite asStreamTableWrite(){
        return this;
    }
}
