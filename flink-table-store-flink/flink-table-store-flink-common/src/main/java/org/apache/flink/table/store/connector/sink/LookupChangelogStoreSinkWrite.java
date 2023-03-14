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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.table.FileStoreTable;

import java.io.IOException;
import java.util.List;

/**
 * {@link StoreSinkWrite} for {@link CoreOptions.ChangelogProducer#LOOKUP} changelog producer. This
 * writer will wait compaction in {@link #prepareCommit}.
 */
public class LookupChangelogStoreSinkWrite extends StoreSinkWriteImpl {

    public LookupChangelogStoreSinkWrite(
            FileStoreTable table,
            StateInitializationContext context,
            String initialCommitUser,
            IOManager ioManager,
            boolean isOverwrite)
            throws Exception {
        super(table, context, initialCommitUser, ioManager, isOverwrite);
    }

    @Override
    public List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        return super.prepareCommit(true, checkpointId);
    }
}
