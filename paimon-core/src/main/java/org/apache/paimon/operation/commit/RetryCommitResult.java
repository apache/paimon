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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.SimpleFileEntry;

import java.util.List;

/** Need to retry commit of {@link CommitResult}. */
public class RetryCommitResult implements CommitResult {

    public final Snapshot latestSnapshot;
    public final List<SimpleFileEntry> baseDataFiles;
    public final Exception exception;

    public RetryCommitResult(
            Snapshot latestSnapshot, List<SimpleFileEntry> baseDataFiles, Exception exception) {
        this.latestSnapshot = latestSnapshot;
        this.baseDataFiles = baseDataFiles;
        this.exception = exception;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }
}
