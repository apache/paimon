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

package org.apache.flink.table.store.file.compact;

import org.apache.flink.table.store.file.io.DataFileMeta;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

/** A {@link CompactManager} which only performs compaction when forced by user. */
public class LazyCompactManager implements CompactManager {

    private final CompactManager wrapped;

    public LazyCompactManager(CompactManager wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean shouldWaitCompaction() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        wrapped.addNewFile(file);
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (fullCompaction) {
            wrapped.triggerCompaction(true);
        }
    }

    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        return wrapped.getCompactionResult(blocking);
    }

    @Override
    public void cancelCompaction() {
        wrapped.cancelCompaction();
    }
}
