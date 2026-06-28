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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Compact manifest procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.compact_manifest(table => 'tableId')
 *  CALL sys.compact_manifest(table => 'tableId', dry_run => true)
 * </code></pre>
 */
public class CompactManifestProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional("dry_run", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.StringType, true, Metadata.empty())
                    });

    protected CompactManifestProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {

        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String options = args.isNullAt(1) ? null : args.getString(1);
        boolean dryRun = !args.isNullAt(2) && args.getBoolean(2);

        Table table = loadSparkTable(tableIdent).getTable();
        HashMap<String, String> dynamicOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(dynamicOptions, options);
        table = table.copy(dynamicOptions);

        if (dryRun) {
            String result = dryRunCompactManifest((FileStoreTable) table);
            return new InternalRow[] {newInternalRow(UTF8String.fromString(result))};
        }

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new InternalRow[] {newInternalRow(UTF8String.fromString("success"))};
    }

    private String dryRunCompactManifest(FileStoreTable table) {
        Snapshot latestSnapshot = table.store().snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            return "Dry run: no snapshot exists, nothing to compact.";
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> beforeManifests = manifestList.readDataManifests(latestSnapshot);

        Options compactOptions = Options.fromMap(table.options());
        compactOptions.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 1);
        compactOptions.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE, MemorySize.ofBytes(1));

        List<ManifestFileMeta> afterManifests =
                ManifestFileMerger.merge(
                        beforeManifests,
                        table.store().manifestFileFactory().create(),
                        table.schema().logicalPartitionType(),
                        new CoreOptions(compactOptions),
                        null);

        long beforeFileCount = beforeManifests.size();
        long afterFileCount = afterManifests.size();
        long beforeTotalSize = beforeManifests.stream().mapToLong(ManifestFileMeta::fileSize).sum();
        long afterTotalSize = afterManifests.stream().mapToLong(ManifestFileMeta::fileSize).sum();
        long beforeDeletedEntries =
                beforeManifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum();
        long afterDeletedEntries =
                afterManifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum();
        long eliminatedDeletedEntries = beforeDeletedEntries - afterDeletedEntries;

        return String.format(
                "Dry run: manifest compaction would reduce %d manifest files to %d, "
                        + "total file size from %s to %s, "
                        + "eliminating %d deleted entries.",
                beforeFileCount,
                afterFileCount,
                MemorySize.ofBytes(beforeTotalSize),
                MemorySize.ofBytes(afterTotalSize),
                eliminatedDeletedEntries);
    }

    @Override
    public String description() {
        return "This procedure execute compact action on paimon table.";
    }

    public static ProcedureBuilder builder() {
        return new Builder<CompactManifestProcedure>() {
            @Override
            public CompactManifestProcedure doBuild() {
                return new CompactManifestProcedure(tableCatalog());
            }
        };
    }
}
