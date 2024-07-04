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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.OrphanFilesClean;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.operation.OrphanFilesClean.SHOW_LIMIT;

/** Action to remove the orphan data files and metadata files. */
public class RemoveOrphanFilesAction extends ActionBase {

    private final List<Pair<String, OrphanFilesClean>> tableOrphanFilesCleans;

    public RemoveOrphanFilesAction(
            String warehouse,
            String databaseName,
            @Nullable String tableName,
            Map<String, String> catalogConfig)
            throws Catalog.TableNotExistException, Catalog.DatabaseNotExistException {
        super(warehouse, catalogConfig);
        this.tableOrphanFilesCleans =
                OrphanFilesClean.constructOrphanFilesCleans(catalog, databaseName, tableName);
    }

    public void olderThan(String olderThan) {
        OrphanFilesClean.initOlderThan(olderThan, this.tableOrphanFilesCleans);
    }

    public void dryRun() {
        OrphanFilesClean.initDryRun(this.tableOrphanFilesCleans);
    }

    public static String[] executeOrphanFilesClean(
            List<Pair<String, OrphanFilesClean>> tableOrphanFilesCleans)
            throws ExecutionException, InterruptedException {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ExecutorService executePool =
                new ThreadPoolExecutor(
                        availableProcessors,
                        availableProcessors,
                        1,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-RemoveOrphanFiles"));

        List<Future<List<Path>>> tasks = new ArrayList<>();
        for (Pair<String, OrphanFilesClean> tableOrphanFilesClean : tableOrphanFilesCleans) {
            OrphanFilesClean orphanFilesClean = tableOrphanFilesClean.getRight();
            Future<List<Path>> task =
                    executePool.submit(
                            () -> {
                                try {
                                    return orphanFilesClean.clean();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
            tasks.add(task);
        }

        List<Path> cleanOrphanFiles = new ArrayList<>();
        for (Future<List<Path>> task : tasks) {
            cleanOrphanFiles.addAll(task.get());
        }

        executePool.shutdownNow();

        return OrphanFilesClean.showDeletedFiles(cleanOrphanFiles, SHOW_LIMIT)
                .toArray(new String[0]);
    }

    @Override
    public void run() throws Exception {
        executeOrphanFilesClean(tableOrphanFilesCleans);
    }
}
