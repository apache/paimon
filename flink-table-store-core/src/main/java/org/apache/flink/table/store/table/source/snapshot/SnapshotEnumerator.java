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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.table.source.DataTableScan.DataFilePlan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Enumerate incremental changes from newly created snapshots. */
public interface SnapshotEnumerator {

    /**
     * The first call to this method will produce a {@link PlanResult} containing the base files for
     * the following incremental changes (or containing null if there are no base files).
     *
     * <p>Following calls to this method will produce {@link PlanResult}s containing incremental
     * changed files. If there is currently no newer snapshots, {@link PlanResult} with null plan
     * will be returned instead.
     *
     * <p>Returning {@link FinishedResult} if this enumerator is finished.
     */
    Result enumerate();

    default Result planResult(@Nullable DataFilePlan plan) {
        return new PlanResult(plan);
    }

    default Result finishedResult() {
        return new FinishedResult();
    }

    default List<DataFilePlan> enumerateAll() {
        List<DataFilePlan> plans = new ArrayList<>();
        while (true) {
            Result result = enumerate();
            if (result instanceof FinishedResult) {
                break;
            }
            DataFilePlan plan = result.plan();
            if (plan == null) {
                continue;
            }
            plans.add(plan);
        }
        return plans;
    }

    /** Result of enumeration. */
    interface Result {
        @Nullable
        DataFilePlan plan();
    }

    /** Result for a plan. */
    class PlanResult implements Result {

        private final DataFilePlan plan;

        public PlanResult(DataFilePlan plan) {
            this.plan = plan;
        }

        @Nullable
        @Override
        public DataFilePlan plan() {
            return plan;
        }
    }

    /** Result for finished. */
    class FinishedResult implements Result {

        @Nullable
        @Override
        public DataFilePlan plan() {
            throw new RuntimeException("This is a finished result.");
        }
    }
}
