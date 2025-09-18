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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Compact fields procedure for data-evolution table. */
public class CompactFieldsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CompactFieldsProcedure(TableCatalog tableCatalog) {
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
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(table instanceof FileStoreTable);
                    InternalRow internalRow = newInternalRow(execute((FileStoreTable) table));
                    return new InternalRow[] {internalRow};
                });
    }

    @Override
    public String description() {
        return "This procedure execute compact fields action on data-evolution enabled paimon table.";
    }

    private boolean execute(FileStoreTable table) {
        try {
            JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());

            List<ManifestEntry> dataFileMetas =
                    table.store()
                            .newScan()
                            .withManifestEntryFilter(f -> f.file().firstRowId() != null)
                            .plan()
                            .files();

            String commitUser = createCommitUser(table.coreOptions().toConfiguration());
            final BinaryRowSerializer binaryRowSerializer =
                    new BinaryRowSerializer(table.partitionKeys().size());
            List<Pair<byte[], byte[]>> toCompact =
                    pickWaitCompact(dataFileMetas, binaryRowSerializer);
            javaSparkContext
                    .parallelize(toCompact, toCompact.size())
                    .map(
                            pair -> {
                                DataFileMetaSerializer dataFileMetaSerializer =
                                        new DataFileMetaSerializer();
                                byte[] partitionBytes = pair.getLeft();
                                DataInputDeserializer dataInputDeserializer =
                                        new DataInputDeserializer(partitionBytes);
                                BinaryRow partition =
                                        binaryRowSerializer.deserialize(dataInputDeserializer);
                                List<DataFileMeta> files =
                                        dataFileMetaSerializer.deserializeList(pair.getRight());
                                long firstRowId = files.get(0).firstRowId();
                                FileStorePathFactory pathFactory = table.store().pathFactory();
                                DataSplit.Builder builder = DataSplit.builder();
                                builder.withPartition(partition);
                                builder.withDataFiles(files);
                                builder.withBucket(0);
                                builder.withBucketPath(
                                        pathFactory.bucketPath(partition, 0).toString());

                                try (BatchTableWrite write =
                                        table.copy(
                                                        Collections.singletonMap(
                                                                CoreOptions.TARGET_FILE_SIZE.key(),
                                                                "9999999 GB"))
                                                .newWrite(commitUser)) {
                                    table.newRead()
                                            .createReader(builder.build())
                                            .forEachRemaining(
                                                    row -> {
                                                        try {
                                                            write.write(row);
                                                        } catch (Exception e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                    });

                                    List<CommitMessage> commitMessages = write.prepareCommit();
                                    assert commitMessages.size() == 1;
                                    CommitMessageImpl result =
                                            (CommitMessageImpl) commitMessages.get(0);
                                    List<DataFileMeta> dataFiles =
                                            result.newFilesIncrement().newFiles();
                                    assert dataFiles.size() == 1;
                                    DataFileMeta dataFileMeta =
                                            dataFiles.get(0).assignFirstRowId(firstRowId);
                                    CommitMessage commitMessage =
                                            new CommitMessageImpl(
                                                    partition,
                                                    0,
                                                    null,
                                                    DataIncrement.emptyIncrement(),
                                                    new CompactIncrement(
                                                            files,
                                                            Collections.singletonList(dataFileMeta),
                                                            Collections.emptyList()));

                                    CommitMessageSerializer messageSer =
                                            new CommitMessageSerializer();
                                    return messageSer.serialize(commitMessage);
                                }
                            })
                    .repartition(1)
                    .foreachPartition(
                            it -> {
                                List<CommitMessage> messages = new ArrayList<>();
                                CommitMessageSerializer messageSer = new CommitMessageSerializer();
                                while (it.hasNext()) {
                                    byte[] messageBytes = it.next();
                                    messages.add(
                                            messageSer.deserialize(
                                                    messageSer.getVersion(), messageBytes));
                                }
                                try (TableCommitImpl commit = table.newCommit(commitUser)) {
                                    commit.commit(messages);
                                }
                            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to compact fields", e);
        }

        return true;
    }

    private List<Pair<byte[], byte[]>> pickWaitCompact(
            List<ManifestEntry> entries, BinaryRowSerializer binaryRowSerializer)
            throws IOException {
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(1024);
        DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();
        entries.sort(Comparator.comparing(o -> o.file().firstRowId()));
        List<Pair<byte[], byte[]>> waitCompat = new ArrayList<>();
        List<DataFileMeta> files = new ArrayList<>();
        long lastFirstRowId = Long.MIN_VALUE;
        BinaryRow lastPartition = null;
        for (ManifestEntry entry : entries) {
            if (lastFirstRowId == entry.file().firstRowId()) {
                files.add(entry.file());
                continue;
            }
            if (!files.isEmpty()) {
                if (files.size() > 1) {
                    dataOutputSerializer.clear();
                    binaryRowSerializer.serialize(lastPartition, dataOutputSerializer);
                    waitCompat.add(
                            Pair.of(
                                    dataOutputSerializer.getCopyOfBuffer(),
                                    dataFileMetaSerializer.serializeList(files)));
                }
                files = new ArrayList<>();
            }
            files.add(entry.file());
            lastFirstRowId = entry.file().firstRowId();
            lastPartition = entry.partition();
        }
        if (files.size() > 1) {
            dataOutputSerializer.clear();
            binaryRowSerializer.serialize(lastPartition, dataOutputSerializer);
            waitCompat.add(
                    Pair.of(
                            dataOutputSerializer.getCopyOfBuffer(),
                            dataFileMetaSerializer.serializeList(files)));
        }
        return waitCompat;
    }

    public static ProcedureBuilder builder() {
        return new Builder<CompactFieldsProcedure>() {
            @Override
            public CompactFieldsProcedure doBuild() {
                return new CompactFieldsProcedure(tableCatalog());
            }
        };
    }
}
