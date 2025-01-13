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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** {@link Source} that does not require coordination between JobManager and TaskManagers. */
public abstract class AbstractNonCoordinatedSource<T>
        implements Source<T, SimpleSourceSplit, NoOpEnumState> {
    @Override
    public SplitEnumerator<SimpleSourceSplit, NoOpEnumState> createEnumerator(
            SplitEnumeratorContext<SimpleSourceSplit> enumContext) {
        return new NoOpEnumerator<>();
    }

    @Override
    public SplitEnumerator<SimpleSourceSplit, NoOpEnumState> restoreEnumerator(
            SplitEnumeratorContext<SimpleSourceSplit> enumContext, NoOpEnumState checkpoint) {
        return new NoOpEnumerator<>();
    }

    @Override
    public SimpleVersionedSerializer<SimpleSourceSplit> getSplitSerializer() {
        return new SimpleSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<NoOpEnumState> getEnumeratorCheckpointSerializer() {
        return new NoOpEnumStateSerializer();
    }
}
