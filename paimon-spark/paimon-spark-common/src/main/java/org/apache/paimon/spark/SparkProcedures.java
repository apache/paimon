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

import org.apache.paimon.spark.procedure.AlterFunctionProcedure;
import org.apache.paimon.spark.procedure.AlterViewDialectProcedure;
import org.apache.paimon.spark.procedure.ClearConsumersProcedure;
import org.apache.paimon.spark.procedure.CompactDatabaseProcedure;
import org.apache.paimon.spark.procedure.CompactManifestProcedure;
import org.apache.paimon.spark.procedure.CompactProcedure;
import org.apache.paimon.spark.procedure.CopyFilesProcedure;
import org.apache.paimon.spark.procedure.CreateBranchProcedure;
import org.apache.paimon.spark.procedure.CreateFunctionProcedure;
import org.apache.paimon.spark.procedure.CreateGlobalIndexProcedure;
import org.apache.paimon.spark.procedure.CreateTagFromTimestampProcedure;
import org.apache.paimon.spark.procedure.CreateTagProcedure;
import org.apache.paimon.spark.procedure.DeleteBranchProcedure;
import org.apache.paimon.spark.procedure.DeleteTagProcedure;
import org.apache.paimon.spark.procedure.DropFunctionProcedure;
import org.apache.paimon.spark.procedure.ExpirePartitionsProcedure;
import org.apache.paimon.spark.procedure.ExpireSnapshotsProcedure;
import org.apache.paimon.spark.procedure.ExpireTagsProcedure;
import org.apache.paimon.spark.procedure.FastForwardProcedure;
import org.apache.paimon.spark.procedure.MarkPartitionDoneProcedure;
import org.apache.paimon.spark.procedure.MigrateDatabaseProcedure;
import org.apache.paimon.spark.procedure.MigrateTableProcedure;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.spark.procedure.ProcedureBuilder;
import org.apache.paimon.spark.procedure.PurgeFilesProcedure;
import org.apache.paimon.spark.procedure.RemoveOrphanFilesProcedure;
import org.apache.paimon.spark.procedure.RemoveUnexistingFilesProcedure;
import org.apache.paimon.spark.procedure.RenameTagProcedure;
import org.apache.paimon.spark.procedure.RepairProcedure;
import org.apache.paimon.spark.procedure.ReplaceTagProcedure;
import org.apache.paimon.spark.procedure.RescaleProcedure;
import org.apache.paimon.spark.procedure.ResetConsumerProcedure;
import org.apache.paimon.spark.procedure.RewriteFileIndexProcedure;
import org.apache.paimon.spark.procedure.RollbackProcedure;
import org.apache.paimon.spark.procedure.RollbackToTimestampProcedure;
import org.apache.paimon.spark.procedure.RollbackToWatermarkProcedure;
import org.apache.paimon.spark.procedure.TriggerTagAutomaticCreationProcedure;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/** The {@link Procedure}s including all the stored procedures. */
public class SparkProcedures {

    private static final Map<String, Supplier<ProcedureBuilder>> BUILDERS = initProcedureBuilders();

    private SparkProcedures() {}

    public static ProcedureBuilder newBuilder(String name) {
        Supplier<ProcedureBuilder> builderSupplier = BUILDERS.get(name.toLowerCase(Locale.ROOT));
        return builderSupplier != null ? builderSupplier.get() : null;
    }

    public static Set<String> names() {
        return BUILDERS.keySet();
    }

    private static Map<String, Supplier<ProcedureBuilder>> initProcedureBuilders() {
        ImmutableMap.Builder<String, Supplier<ProcedureBuilder>> procedureBuilders =
                ImmutableMap.builder();
        procedureBuilders.put("rollback", RollbackProcedure::builder);
        procedureBuilders.put("rollback_to_timestamp", RollbackToTimestampProcedure::builder);
        procedureBuilders.put("rollback_to_watermark", RollbackToWatermarkProcedure::builder);
        procedureBuilders.put("purge_files", PurgeFilesProcedure::builder);
        procedureBuilders.put("create_tag", CreateTagProcedure::builder);
        procedureBuilders.put("replace_tag", ReplaceTagProcedure::builder);
        procedureBuilders.put("rename_tag", RenameTagProcedure::builder);
        procedureBuilders.put(
                "create_tag_from_timestamp", CreateTagFromTimestampProcedure::builder);
        procedureBuilders.put("delete_tag", DeleteTagProcedure::builder);
        procedureBuilders.put("expire_tags", ExpireTagsProcedure::builder);
        procedureBuilders.put("create_branch", CreateBranchProcedure::builder);
        procedureBuilders.put("create_global_index", CreateGlobalIndexProcedure::builder);
        procedureBuilders.put("delete_branch", DeleteBranchProcedure::builder);
        procedureBuilders.put("compact", CompactProcedure::builder);
        procedureBuilders.put("compact_database", CompactDatabaseProcedure::builder);
        procedureBuilders.put("rescale", RescaleProcedure::builder);
        procedureBuilders.put("migrate_database", MigrateDatabaseProcedure::builder);
        procedureBuilders.put("migrate_table", MigrateTableProcedure::builder);
        procedureBuilders.put("remove_orphan_files", RemoveOrphanFilesProcedure::builder);
        procedureBuilders.put("remove_unexisting_files", RemoveUnexistingFilesProcedure::builder);
        procedureBuilders.put("expire_snapshots", ExpireSnapshotsProcedure::builder);
        procedureBuilders.put("expire_partitions", ExpirePartitionsProcedure::builder);
        procedureBuilders.put("repair", RepairProcedure::builder);
        procedureBuilders.put("fast_forward", FastForwardProcedure::builder);
        procedureBuilders.put("reset_consumer", ResetConsumerProcedure::builder);
        procedureBuilders.put("mark_partition_done", MarkPartitionDoneProcedure::builder);
        procedureBuilders.put("compact_manifest", CompactManifestProcedure::builder);
        procedureBuilders.put("clear_consumers", ClearConsumersProcedure::builder);
        procedureBuilders.put("alter_view_dialect", AlterViewDialectProcedure::builder);
        procedureBuilders.put("create_function", CreateFunctionProcedure::builder);
        procedureBuilders.put("alter_function", AlterFunctionProcedure::builder);
        procedureBuilders.put("drop_function", DropFunctionProcedure::builder);
        procedureBuilders.put(
                "trigger_tag_automatic_creation", TriggerTagAutomaticCreationProcedure::builder);
        procedureBuilders.put("rewrite_file_index", RewriteFileIndexProcedure::builder);
        procedureBuilders.put("copy", CopyFilesProcedure::builder);
        return procedureBuilders.build();
    }
}
