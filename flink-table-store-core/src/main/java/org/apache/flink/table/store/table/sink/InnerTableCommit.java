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

import org.apache.flink.table.store.file.operation.Lock;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Inner {@link TableCommit} contains overwrite setter. */
public interface InnerTableCommit extends StreamTableCommit, BatchTableCommit {

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    default InnerTableCommit withOverwrite(@Nullable Map<String, String> staticPartition) {
        if (staticPartition != null) {
            withOverwrite(Collections.singletonList(staticPartition));
        }
        return this;
    }

    InnerTableCommit withOverwrite(@Nullable List<Map<String, String>> overwritePartitions);

    /** @deprecated lock should pass from table. */
    @Deprecated
    InnerTableCommit withLock(Lock lock);
}
