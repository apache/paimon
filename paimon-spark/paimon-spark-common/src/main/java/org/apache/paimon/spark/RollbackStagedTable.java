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

package org.apache.paimon.spark;

import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/** A staged table that rolls back by invoking the provided abort action. */
class RollbackStagedTable implements StagedTable, SupportsRead, SupportsWrite, SupportsDelete {

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    @FunctionalInterface
    interface StagedAction {
        void run(RollbackStagedTable stagedTable) throws Exception;
    }

    private static final Action DO_NOTHING = () -> {};

    private final Table table;
    private final StagedAction commitAction;
    private final Action abortAction;
    private boolean committed;
    private boolean aborted;
    private boolean writeStarted;

    RollbackStagedTable(Table table, Action abortAction) {
        this(table, DO_NOTHING, abortAction);
    }

    RollbackStagedTable(Table table, Action commitAction, Action abortAction) {
        this(table, ignored -> commitAction.run(), abortAction);
    }

    RollbackStagedTable(Table table, StagedAction commitAction, Action abortAction) {
        this.table = table;
        this.commitAction = commitAction;
        this.abortAction = abortAction;
    }

    @Override
    public String name() {
        return table.name();
    }

    @Override
    public StructType schema() {
        return table.schema();
    }

    @Override
    public Transform[] partitioning() {
        return table.partitioning();
    }

    @Override
    public Map<String, String> properties() {
        return table.properties();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return table.capabilities();
    }

    @Override
    public void deleteWhere(Filter[] filters) {
        call(SupportsDelete.class, t -> t.deleteWhere(filters));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return callReturning(SupportsRead.class, t -> t.newScanBuilder(options));
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        writeStarted = true;
        return callReturning(SupportsWrite.class, t -> t.newWriteBuilder(info));
    }

    boolean hasWriteStarted() {
        return writeStarted;
    }

    @Override
    public void commitStagedChanges() {
        if (committed || aborted) {
            return;
        }

        try {
            commitAction.run(this);
            committed = true;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortStagedChanges() {
        if (committed || aborted) {
            return;
        }

        try {
            abortAction.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            aborted = true;
        }
    }

    private <T> void call(Class<? extends T> requiredClass, Consumer<T> task) {
        callReturning(
                requiredClass,
                instance -> {
                    task.accept(instance);
                    return null;
                });
    }

    private <T, R> R callReturning(Class<? extends T> requiredClass, Function<T, R> task) {
        if (requiredClass.isInstance(table)) {
            return task.apply(requiredClass.cast(table));
        }

        throw new UnsupportedOperationException(
                String.format(
                        "Table does not implement %s: %s (%s)",
                        requiredClass.getSimpleName(), table.name(), table.getClass().getName()));
    }
}
