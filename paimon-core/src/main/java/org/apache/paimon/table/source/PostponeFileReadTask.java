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

package org.apache.paimon.table.source;

import org.apache.paimon.table.BucketMode;

import java.io.Serializable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A coordinator-planned read of one physical postpone file.
 *
 * <p>This is deliberately not a {@link Split}. It carries deterministic replay metadata for the
 * specialized postpone read path and is never passed to generic {@link TableRead#createReader}.
 */
public final class PostponeFileReadTask implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataSplit split;
    private final long replaySequenceBase;

    public PostponeFileReadTask(DataSplit split, long replaySequenceBase) {
        checkArgument(
                !split.isStreaming()
                        && split.bucket() == BucketMode.POSTPONE_BUCKET
                        && split.dataFiles().size() == 1,
                "Postpone file read task requires one batch DataSplit containing one postpone file.");
        checkArgument(
                replaySequenceBase >= 0, "Postpone replay sequence base must be non-negative.");
        this.split = split;
        this.replaySequenceBase = replaySequenceBase;
    }

    public DataSplit split() {
        return split;
    }

    public long replaySequenceBase() {
        return replaySequenceBase;
    }
}
