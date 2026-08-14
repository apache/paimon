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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestEntrySortKey;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFile.ManifestEntryWriter;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestFileMetaSerializer;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Rewrite manifest procedure. It reads all manifest entries, sorts them globally by {@code
 * partition -> bucket -> level -> fileName} (canceling ADD/DELETE pairs along the way) and writes
 * them back as new manifest files so that {@link ManifestFileMeta} statistics become more compact
 * for scan pruning. The sort runs distributed via a Spark {@code sortByKey} shuffle.
 *
 * <p>An optional {@code where} clause restricts the rewrite to manifests whose partition stats may
 * match the predicate; the remaining manifests are left untouched.
 *
 * <pre><code>
 *  CALL sys.rewrite_manifest(table => 'tableId')
 *  CALL sys.rewrite_manifest(table => 'tableId', where => 'dt = "2024-01-01"')
 * </code></pre>
 */
public class RewriteManifestProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "rewritten_manifests_count",
                                DataTypes.IntegerType,
                                true,
                                Metadata.empty()),
                        new StructField(
                                "added_manifests_count",
                                DataTypes.IntegerType,
                                true,
                                Metadata.empty())
                    });

    protected RewriteManifestProcedure(TableCatalog tableCatalog) {
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
        String where = args.isNullAt(1) ? null : args.getString(1);
        String options = args.isNullAt(2) ? null : args.getString(2);

        Table table = loadSparkTable(tableIdent).getTable();
        HashMap<String, String> dynamicOptions = new HashMap<>();
        ProcedureUtils.putAllOptions(dynamicOptions, options);
        FileStoreTable fileStoreTable = (FileStoreTable) table.copy(dynamicOptions);

        // 1. read the latest snapshot and its data manifests
        Snapshot latestSnapshot = fileStoreTable.store().snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            return new InternalRow[] {newInternalRow(0, 0)};
        }
        ManifestList manifestList = fileStoreTable.store().manifestListFactory().create();
        List<ManifestFileMeta> currentManifests = manifestList.readDataManifests(latestSnapshot);
        if (currentManifests.isEmpty()) {
            return new InternalRow[] {newInternalRow(0, 0)};
        }

        // 2. filter manifests by the optional where clause (partition-stats pruning)
        List<ManifestFileMeta> manifestsToRewrite = currentManifests;
        PartitionPredicate partitionPredicate = resolvePartitionPredicate(tableIdent, table, where);
        if (partitionPredicate != null) {
            manifestsToRewrite = filterManifests(manifestsToRewrite, partitionPredicate);
            if (manifestsToRewrite.isEmpty()) {
                return new InternalRow[] {newInternalRow(0, 0)};
            }
        }

        // 3. globally sort manifest entries and write them back as new manifest files
        List<ManifestFileMeta> newManifests = rewriteManifests(fileStoreTable, manifestsToRewrite);

        // 4. commit the rewritten manifests (optimistic concurrency with retry)
        try (BatchTableCommit commit = fileStoreTable.newBatchWriteBuilder().newCommit()) {
            commit.replaceManifests(manifestsToRewrite, newManifests);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // rewritten_manifests_count: number of manifests that were rewritten (input)
        // added_manifests_count: number of new manifests produced by the rewrite (output)
        int rewrittenCount = manifestsToRewrite.size();
        int addedCount = newManifests.size();
        return new InternalRow[] {newInternalRow(rewrittenCount, addedCount)};
    }

    /**
     * Parse the {@code where} SQL string into a {@link PartitionPredicate}, validating that it only
     * references partition columns. Returns {@code null} when {@code where} is blank.
     */
    private PartitionPredicate resolvePartitionPredicate(
            Identifier tableIdent, Table table, String where) {
        if (StringUtils.isNullOrWhitespaceOnly(where)) {
            return null;
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        DataSourceV2Relation relation = createRelation(tableIdent);
        Expression condition = ExpressionUtils.resolveFilter(spark(), relation, where);
        checkArgument(
                ExpressionUtils.isValidPredicate(
                        spark(), condition, fileStoreTable.partitionKeys().toArray(new String[0])),
                "Only partition predicate is supported, your predicate is %s, but partition keys are %s",
                condition,
                fileStoreTable.partitionKeys());
        Predicate predicate =
                ExpressionUtils.convertConditionToPaimonPredicate(
                                condition,
                                ((LogicalPlan) relation).output(),
                                table.rowType(),
                                false)
                        .getOrElse(null);

        // the predicate references fields by their index in the full row type; map them to the
        // partition type index so PartitionPredicate can test against partitionStats / BinaryRow
        // partitions (which only contain partition columns)
        List<String> partitionKeys = fileStoreTable.partitionKeys();
        int[] fieldIdxToPartitionIdx =
                fileStoreTable.schema().fields().stream()
                        .mapToInt(f -> partitionKeys.indexOf(f.name()))
                        .toArray();
        Predicate partitionPredicate =
                PredicateBuilder.transformFieldMapping(predicate, fieldIdxToPartitionIdx)
                        .orElse(null);

        RowType partitionType = fileStoreTable.store().partitionType();
        return PartitionPredicate.fromPredicate(partitionType, partitionPredicate);
    }

    /** Keep only manifests whose partition stats may match the predicate. */
    private List<ManifestFileMeta> filterManifests(
            List<ManifestFileMeta> manifests, PartitionPredicate predicate) {
        return manifests.stream()
                .filter(
                        m -> {
                            SimpleStats stats = m.partitionStats();
                            return predicate.test(
                                    m.numAddedFiles() + m.numDeletedFiles(),
                                    stats.minValues(),
                                    stats.maxValues(),
                                    stats.nullCounts());
                        })
                .collect(Collectors.toList());
    }

    private List<ManifestFileMeta> rewriteManifests(
            FileStoreTable table, List<ManifestFileMeta> currentManifests) {
        List<DataType> partitionFieldTypes = table.store().partitionType().getFieldTypes();
        ManifestFileMetaSerializer metaSerializer = new ManifestFileMetaSerializer();

        // serialize ManifestFileMeta to byte[] so they can travel through the RDD
        List<byte[]> serializedMetas = new ArrayList<>(currentManifests.size());
        for (ManifestFileMeta meta : currentManifests) {
            try {
                serializedMetas.add(metaSerializer.serializeToBytes(meta));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        int numPartitions = computeNumPartitions(table, currentManifests);

        JavaSparkContext jsc = new JavaSparkContext(spark().sparkContext());

        // The table is captured by the closure (not broadcast) so that Spark serializes it with
        // Java serialization (ClosureCleaner always uses JavaSerializer). This is required because
        // the table's FileIO (e.g. HadoopFileIO) holds a SerializableConfiguration whose transient
        // Configuration is restored only via Java writeObject/readObject — Kryo (used by broadcast
        // when spark.serializer=KryoSerializer) would skip those callbacks and leave a null conf.
        FileStoreTable tableRef = table;

        // Step 1: read all entries (ADD and DELETE) and pair them with their sort key
        JavaPairRDD<ManifestEntrySortKey, byte[]> entryRDD =
                jsc.parallelize(serializedMetas, Math.min(serializedMetas.size(), numPartitions))
                        .flatMapToPair(
                                (PairFlatMapFunction<byte[], ManifestEntrySortKey, byte[]>)
                                        manifestBytes -> {
                                            ManifestFileMetaSerializer ser =
                                                    new ManifestFileMetaSerializer();
                                            ManifestFileMeta manifest =
                                                    ser.deserializeFromBytes(manifestBytes);
                                            ManifestFile manifestFile =
                                                    tableRef.store().manifestFileFactory().create();
                                            ManifestEntrySerializer entrySer =
                                                    new ManifestEntrySerializer();

                                            List<Tuple2<ManifestEntrySortKey, byte[]>> pairs =
                                                    new ArrayList<>();
                                            for (ManifestEntry entry :
                                                    manifestFile.read(
                                                            manifest.fileName(),
                                                            manifest.fileSize())) {
                                                ManifestEntrySortKey key =
                                                        new ManifestEntrySortKey(
                                                                entry.partition(),
                                                                entry.bucket(),
                                                                entry.file().level(),
                                                                entry.fileName(),
                                                                partitionFieldTypes);
                                                pairs.add(
                                                        new Tuple2<>(
                                                                key,
                                                                entrySer.serializeToBytes(entry)));
                                            }
                                            return pairs.iterator();
                                        });

        // Step 2: global sort. The sort key is (partition, bucket, level, fileName), so
        // the ADD and DELETE of the same file are adjacent.
        JavaPairRDD<ManifestEntrySortKey, byte[]> sortedRDD =
                entryRDD.sortByKey(true, numPartitions);

        // Step 3: within each partition, stream the sorted entries, cancel ADD/DELETE pairs per
        // manifest entry, and write surviving entries to a single manifest file. Because the sort
        // key
        // is (partition, bucket, level, fileName), entries of the same file are consecutive; the
        // per-key buffer holds at most one ADD and one DELETE, so memory is negligible. Each task
        // produces at most one manifest, sized roughly to the manifest target file size.
        List<byte[]> serializedResult =
                sortedRDD
                        .mapPartitions(
                                (FlatMapFunction<
                                                Iterator<Tuple2<ManifestEntrySortKey, byte[]>>,
                                                byte[]>)
                                        iter -> {
                                            FileStoreTable t = tableRef;
                                            FileStorePathFactory pathFactory =
                                                    t.store().pathFactory();
                                            Path manifestPath = pathFactory.newManifestFile();
                                            ManifestFile manifestFile =
                                                    t.store().manifestFileFactory().create();
                                            ManifestEntryWriter writer =
                                                    manifestFile.createManifestEntryWriter(
                                                            manifestPath);
                                            ManifestEntrySerializer entrySer =
                                                    new ManifestEntrySerializer();

                                            // per-current-key buffer: at most one ADD and one
                                            // DELETE for the same Identifier
                                            ManifestEntrySortKey currentKey = null;
                                            ManifestEntry bufferedAdd = null;
                                            ManifestEntry bufferedDelete = null;
                                            try {
                                                while (iter.hasNext()) {
                                                    Tuple2<ManifestEntrySortKey, byte[]> pair =
                                                            iter.next();
                                                    ManifestEntrySortKey key = pair._1;
                                                    ManifestEntry entry =
                                                            entrySer.deserializeFromBytes(pair._2);

                                                    if (currentKey != null
                                                            && currentKey.compareTo(key) != 0) {
                                                        // key changed: flush the previous group
                                                        ManifestEntry survived =
                                                                mergeGroup(
                                                                        bufferedAdd,
                                                                        bufferedDelete);
                                                        if (survived != null) {
                                                            writer.write(survived);
                                                        }
                                                        bufferedAdd = null;
                                                        bufferedDelete = null;
                                                    }

                                                    currentKey = key;
                                                    if (entry.kind() == FileKind.ADD) {
                                                        if (bufferedAdd != null) {
                                                            throw new IllegalStateException(
                                                                    "Duplicate ADD entry for "
                                                                            + entry.identifier());
                                                        }
                                                        bufferedAdd = entry;
                                                    } else {
                                                        if (bufferedDelete != null) {
                                                            throw new IllegalStateException(
                                                                    "Duplicate DELETE entry for "
                                                                            + entry.identifier());
                                                        }
                                                        bufferedDelete = entry;
                                                    }
                                                }

                                                // flush the last group
                                                if (currentKey != null) {
                                                    ManifestEntry survived =
                                                            mergeGroup(bufferedAdd, bufferedDelete);
                                                    if (survived != null) {
                                                        writer.write(survived);
                                                    }
                                                }
                                            } finally {
                                                writer.close();
                                            }

                                            if (writer.recordCount() == 0) {
                                                // nothing survived — delete the empty file and
                                                // emit nothing
                                                manifestFile.delete(writer.path().getName());
                                                return Collections.<byte[]>emptyList().iterator();
                                            }
                                            ManifestFileMeta newMeta = writer.result();
                                            ManifestFileMetaSerializer ser =
                                                    new ManifestFileMetaSerializer();
                                            return Collections.singletonList(
                                                            ser.serializeToBytes(newMeta))
                                                    .iterator();
                                        })
                        .collect();

        List<ManifestFileMeta> newManifests = new ArrayList<>(serializedResult.size());
        for (byte[] bytes : serializedResult) {
            try {
                newManifests.add(metaSerializer.deserializeFromBytes(bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return newManifests;
    }

    /**
     * Merge the ADD and DELETE entries of the same file (same Identifier) in an order-independent
     * way. Returns the surviving ADD if only an ADD is present, {@code null} if the ADD and DELETE
     * cancel each other out, or the DELETE if only a DELETE is present (kept so it can match an ADD
     * in a previous manifest).
     */
    private static ManifestEntry mergeGroup(ManifestEntry addEntry, ManifestEntry deleteEntry) {
        if (addEntry != null && deleteEntry != null) {
            // ADD + DELETE cancel out
            return null;
        }
        if (addEntry != null) {
            return addEntry;
        }
        return deleteEntry;
    }

    /**
     * Estimate the number of output manifests (and thus the sort parallelism) from the input
     * manifests. The surviving entry count is {@code added - deleted} (ADD/DELETE pairs cancel),
     * the average entry size is {@code totalSize / (added + deleted)}, and the estimated output
     * size is {@code avgEntrySize * survivingEntries}. The parallelism is the floor of that over
     * the manifest target file size, so each task produces roughly one manifest of the target size.
     */
    private int computeNumPartitions(FileStoreTable table, List<ManifestFileMeta> manifests) {
        long targetSizeBytes = table.coreOptions().manifestTargetSize().getBytes();
        long totalSizeBytes = 0L;
        long addedEntries = 0L;
        long deletedEntries = 0L;
        for (ManifestFileMeta manifest : manifests) {
            totalSizeBytes += manifest.fileSize();
            addedEntries += manifest.numAddedFiles();
            deletedEntries += manifest.numDeletedFiles();
        }

        if (totalSizeBytes <= 0) {
            throw new IllegalStateException(
                    "Cannot compute parallelism: total manifest size is " + totalSizeBytes);
        }
        long totalEntries = addedEntries + deletedEntries;
        if (totalEntries <= 0) {
            throw new IllegalStateException(
                    "Cannot compute parallelism: total manifest entries is " + totalEntries);
        }
        long survivingEntries = addedEntries - deletedEntries;
        if (survivingEntries < 0) {
            throw new IllegalStateException(
                    "Cannot compute parallelism: surviving entries (added - deleted) is "
                            + survivingEntries);
        }
        double avgEntrySizeBytes = (double) totalSizeBytes / totalEntries;
        long estimatedOutputSizeBytes = (long) (avgEntrySizeBytes * survivingEntries);
        // floor division: prefer fewer, slightly-over-target manifests over more, under-target
        // ones. For example, 35M estimated with 8M target -> 4 manifests (~8.75M each), not 5
        // (~7M each). At least one task is always produced.
        return (int) Math.max(1, estimatedOutputSizeBytes / targetSizeBytes);
    }

    @Override
    public String description() {
        return "This procedure rewrites and globally sorts manifest entries.";
    }

    public static ProcedureBuilder builder() {
        return new Builder<RewriteManifestProcedure>() {
            @Override
            public RewriteManifestProcedure doBuild() {
                return new RewriteManifestProcedure(tableCatalog());
            }
        };
    }
}
