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

package org.apache.flink.table.store.file.data;

import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactRewriter;
import org.apache.flink.table.store.file.compact.CompactStrategy;
import org.apache.flink.table.store.file.compact.CompactUnit;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyCompactManager
        extends CompactManager<List<DataFileMeta>, DataFileMeta, List<CompactUnit>> {

    public AppendOnlyCompactManager(
            ExecutorService executor,
            CompactStrategy<List<DataFileMeta>, List<CompactUnit>> strategy,
            CompactRewriter<DataFileMeta> rewriter) {
        super(executor, strategy, rewriter);
    }

    @Override
    protected Optional<Callable<CompactResult>> createCompactTask(
            List<DataFileMeta> input, List<CompactUnit> output) {
        return Optional.of(new AppendOnlyCompactTask(rewriter, output));
    }
}
