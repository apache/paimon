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

package org.apache.paimon.flink.pipeline.cdc.source.reader;

import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** State of the CDC source reader with independent data row and schema event progress. */
public class CDCSourceSplitState {

    private final TableAwareFileStoreSourceSplit split;

    private long recordsToSkip;

    private long schemaChangeEventsToSkip;

    public CDCSourceSplitState(TableAwareFileStoreSourceSplit split) {
        this.split = checkNotNull(split);
        this.recordsToSkip = split.recordsToSkip();
        this.schemaChangeEventsToSkip = split.schemaChangeEventsToSkip();
    }

    public void setPosition(RecordAndPosition<Event> position) {
        checkArgument(position.getOffset() == CheckpointedPosition.NO_OFFSET);
        if (position.getRecord() instanceof SchemaChangeEvent) {
            schemaChangeEventsToSkip++;
        } else {
            recordsToSkip = position.getRecordSkipCount();
        }
    }

    public long recordsToSkip() {
        return recordsToSkip;
    }

    public long schemaChangeEventsToSkip() {
        return schemaChangeEventsToSkip;
    }

    public TableAwareFileStoreSourceSplit toSourceSplit() {
        return split.updateWithProgress(recordsToSkip, schemaChangeEventsToSkip);
    }

    @Override
    public String toString() {
        return "CDCSourceSplitState{"
                + "split="
                + split
                + ", recordsToSkip="
                + recordsToSkip
                + ", schemaChangeEventsToSkip="
                + schemaChangeEventsToSkip
                + '}';
    }
}
