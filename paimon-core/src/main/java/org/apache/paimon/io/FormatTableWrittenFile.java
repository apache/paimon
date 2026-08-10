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

package org.apache.paimon.io;

import org.apache.paimon.fs.TwoPhaseOutputStream;

/**
 * One data file a format table writer finished, with what it holds. The row count and byte size are
 * counted while writing, so carrying them alongside the committer costs no extra IO and is the only
 * place they can still be had exactly — after the commit the file is just bytes on a path.
 */
public class FormatTableWrittenFile {

    private final TwoPhaseOutputStream.Committer committer;
    private final long recordCount;
    private final long fileSizeInBytes;

    public FormatTableWrittenFile(
            TwoPhaseOutputStream.Committer committer, long recordCount, long fileSizeInBytes) {
        this.committer = committer;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public TwoPhaseOutputStream.Committer committer() {
        return committer;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }
}
