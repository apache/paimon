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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.FileIndexProcessor;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Rewrite file index procedure. */
public class RewriteFileIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(RewriteFileIndexProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("where", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("written_partitions", IntegerType, false, Metadata.empty()),
                        new StructField("written_indexes", IntegerType, false, Metadata.empty())
                    });

    public RewriteFileIndexProcedure(TableCatalog tableCatalog) {
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

        return modifyPaimonTable(
                tableIdent,
                t -> {
                    FileStoreTable table = (FileStoreTable) t;
                    DataSourceV2Relation relation = createRelation(tableIdent);
                    PartitionPredicate partitionPredicate =
                            SparkProcedureUtils.convertToPartitionPredicate(
                                    where,
                                    table.schema().logicalPartitionType(),
                                    spark(),
                                    relation);

                    List<ManifestEntry> manifestEntries =
                            table.store()
                                    .newScan()
                                    .withPartitionFilter(partitionPredicate)
                                    .plan()
                                    .files();

                    if (manifestEntries.isEmpty()) {
                        LOG.info("No files to rewrite.");
                        return new InternalRow[] {newInternalRow(0, 0)};
                    }
                    LOG.info(
                            "Start to rewrite file index, number of manifestEntries is {}",
                            manifestEntries.size());

                    ManifestEntrySerializer manifestSer = new ManifestEntrySerializer();
                    List<byte[]> serEntries = new ArrayList<>();
                    try {
                        for (ManifestEntry manifestEntry : manifestEntries) {
                            serEntries.add(manifestSer.serializeToBytes(manifestEntry));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    JavaSparkContext javaSparkContext =
                            new JavaSparkContext(spark().sparkContext());
                    int readParallelism = readParallelism(serEntries, spark());
                    JavaRDD<byte[]> commitMessageJavaRDD =
                            javaSparkContext
                                    .parallelize(serEntries, readParallelism)
                                    .mapPartitions(new ManifestEntryProcesser(table));

                    Set<BinaryRow> writtenPartitions = new HashSet<>();
                    int writtenIndexes = 0;
                    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
                    try (BatchTableCommit commit = writeBuilder.newCommit()) {
                        CommitMessageSerializer messageSer = new CommitMessageSerializer();
                        List<byte[]> serializedMessages = commitMessageJavaRDD.collect();
                        List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
                        for (byte[] serializedMessage : serializedMessages) {
                            CommitMessageImpl commitMessage =
                                    (CommitMessageImpl)
                                            messageSer.deserialize(
                                                    messageSer.getVersion(), serializedMessage);
                            writtenIndexes +=
                                    commitMessage.compactIncrement().compactAfter().size();
                            writtenPartitions.add(commitMessage.partition());
                            messages.add(commitMessage);
                        }
                        commit.commit(messages);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return new InternalRow[] {
                        newInternalRow(writtenPartitions.size(), writtenIndexes)
                    };
                });
    }

    /** Process manifest entries. */
    public static class ManifestEntryProcesser
            implements FlatMapFunction<Iterator<byte[]>, byte[]> {

        private final FileStoreTable table;

        public ManifestEntryProcesser(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public Iterator<byte[]> call(Iterator<byte[]> entries) throws Exception {
            FileIndexProcessor fileIndexProcessor = new FileIndexProcessor(table);
            List<byte[]> messages = new ArrayList<>();
            CommitMessageSerializer messageSer = new CommitMessageSerializer();
            ManifestEntrySerializer manifestSer = new ManifestEntrySerializer();

            while (entries.hasNext()) {
                ManifestEntry entry = manifestSer.deserializeFromBytes(entries.next());
                BinaryRow partition = entry.partition();
                int bucket = entry.bucket();
                DataFileMeta indexedFile = fileIndexProcessor.process(partition, bucket, entry);

                CommitMessageImpl commitMessage =
                        new CommitMessageImpl(
                                partition,
                                bucket,
                                entry.totalBuckets(),
                                DataIncrement.emptyIncrement(),
                                new CompactIncrement(
                                        Collections.singletonList(entry.file()),
                                        Collections.singletonList(indexedFile),
                                        Collections.emptyList()));
                messages.add(messageSer.serialize(commitMessage));
            }

            return messages.iterator();
        }
    }

    public static ProcedureBuilder builder() {
        return new Builder<RewriteFileIndexProcedure>() {
            @Override
            public RewriteFileIndexProcedure doBuild() {
                return new RewriteFileIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RewriteFileIndexProcedure";
    }
}
