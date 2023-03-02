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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.store.table.source.ReadBuilder;

import javax.annotation.Nullable;

import java.util.Collection;

/** A {@link FlinkSource} for system table. */
public class SimpleSystemSource extends FlinkSource {

    private static final long serialVersionUID = 2L;

    public SimpleSystemSource(ReadBuilder readBuilder, @Nullable Long limit) {
        super(readBuilder, limit);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Collection<FileStoreSourceSplit> splits =
                checkpoint == null
                        ? new FileStoreSourceSplitGenerator()
                                .createSplits(readBuilder.newScan().plan())
                        : checkpoint.splits();

        return new StaticFileStoreSplitEnumerator(context, null, splits);
    }
}
