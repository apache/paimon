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

package org.apache.paimon.flink.source;

import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * State of the reader, essentially a mutable version of the {@link FileStoreSourceSplit}. Has a
 * records-to-skip-count.
 */
public final class FileStoreSourceSplitState {

    private final FileStoreSourceSplit split;

    private long recordsToSkip;

    public FileStoreSourceSplitState(FileStoreSourceSplit split) {
        this.split = checkNotNull(split);
        this.recordsToSkip = split.recordsToSkip();
    }

    public void setPosition(RecordAndPosition<RowData> position) {
        checkArgument(position.getOffset() == CheckpointedPosition.NO_OFFSET);
        this.recordsToSkip = position.getRecordSkipCount();
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    public FileStoreSourceSplit toSourceSplit() {
        return split.updateWithRecordsToSkip(recordsToSkip);
    }
}
