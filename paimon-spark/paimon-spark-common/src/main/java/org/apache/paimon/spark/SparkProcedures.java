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

import org.apache.paimon.spark.procedure.CompactProcedure;
import org.apache.paimon.spark.procedure.CreateBranchProcedure;
import org.apache.paimon.spark.procedure.CreateTagProcedure;
import org.apache.paimon.spark.procedure.DeleteBranchProcedure;
import org.apache.paimon.spark.procedure.DeleteTagProcedure;
import org.apache.paimon.spark.procedure.ExpirePartitionsProcedure;
import org.apache.paimon.spark.procedure.ExpireSnapshotsProcedure;
import org.apache.paimon.spark.procedure.MergeBranchProcedure;
import org.apache.paimon.spark.procedure.MigrateFileProcedure;
import org.apache.paimon.spark.procedure.MigrateTableProcedure;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.spark.procedure.ProcedureBuilder;
import org.apache.paimon.spark.procedure.RemoveOrphanFilesProcedure;
import org.apache.paimon.spark.procedure.RepairProcedure;
import org.apache.paimon.spark.procedure.RollbackProcedure;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/** The {@link Procedure}s including all the stored procedures. */
public class SparkProcedures {

    private static final Map<String, Supplier<ProcedureBuilder>> BUILDERS = initProcedureBuilders();

    private SparkProcedures() {}

    public static ProcedureBuilder newBuilder(String name) {
        Supplier<ProcedureBuilder> builderSupplier = BUILDERS.get(name.toLowerCase(Locale.ROOT));
        return builderSupplier != null ? builderSupplier.get() : null;
    }

    private static Map<String, Supplier<ProcedureBuilder>> initProcedureBuilders() {
        ImmutableMap.Builder<String, Supplier<ProcedureBuilder>> procedureBuilders =
                ImmutableMap.builder();
        procedureBuilders.put("rollback", RollbackProcedure::builder);
        procedureBuilders.put("create_tag", CreateTagProcedure::builder);
        procedureBuilders.put("delete_tag", DeleteTagProcedure::builder);
        procedureBuilders.put("create_branch", CreateBranchProcedure::builder);
        procedureBuilders.put("delete_branch", DeleteBranchProcedure::builder);
        procedureBuilders.put("compact", CompactProcedure::builder);
        procedureBuilders.put("migrate_table", MigrateTableProcedure::builder);
        procedureBuilders.put("migrate_file", MigrateFileProcedure::builder);
        procedureBuilders.put("remove_orphan_files", RemoveOrphanFilesProcedure::builder);
        procedureBuilders.put("expire_snapshots", ExpireSnapshotsProcedure::builder);
        procedureBuilders.put("expire_partitions", ExpirePartitionsProcedure::builder);
        procedureBuilders.put("repair", RepairProcedure::builder);
        procedureBuilders.put("merge_branch", MergeBranchProcedure::builder);
        return procedureBuilders.build();
    }
}
