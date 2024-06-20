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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.FileIndexSink;
import org.apache.paimon.flink.sink.NoneCopyVersionedSerializerTypeSerializerProxy;
import org.apache.paimon.flink.source.FileIndexScanSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.procedure.ProcedureContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;

/** Migrate procedure to migrate hive table to paimon table. */
public class FileIndexProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "file_index_rewrite";
    }

    public String[] call(ProcedureContext procedureContext, String sourceTablePath)
            throws Exception {
        return call(procedureContext, sourceTablePath, "");
    }

    public String[] call(
            ProcedureContext procedureContext, String sourceTablePath, String partitions)
            throws Exception {

        StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
        Table table = catalog.getTable(Identifier.fromString(sourceTablePath));

        List<Map<String, String>> partitionList =
                StringUtils.isBlank(partitions) ? null : getPartitions(partitions.split(";"));

        Predicate partitionPredicate;
        if (partitionList != null) {
            // This predicate is based on the row type of the original table, not bucket table.
            // Because TableScan in BucketsTable is the same with FileStoreTable,
            // and partition filter is done by scan.
            partitionPredicate =
                    PredicateBuilder.or(
                            partitionList.stream()
                                    .map(
                                            p ->
                                                    PredicateBuilder.partition(
                                                            p,
                                                            table.rowType(),
                                                            CoreOptions.PARTITION_DEFAULT_NAME
                                                                    .defaultValue()))
                                    .toArray(Predicate[]::new));
        } else {
            partitionPredicate = null;
        }

        TopoBuilder.build(env, (FileStoreTable) table, partitionPredicate);

        return execute(env, "Add file index for table: " + sourceTablePath);
    }

    private static class TopoBuilder {

        public static void build(
                StreamExecutionEnvironment env,
                FileStoreTable table,
                Predicate partitionPredicate) {
            DataStreamSource<ManifestEntry> source =
                    env.fromSource(
                            new FileIndexScanSource(table, partitionPredicate),
                            WatermarkStrategy.noWatermarks(),
                            "index source",
                            new TypeInfo());
            new FileIndexSink(table).sinkFrom(source);
        }
    }

    private static class TypeInfo extends GenericTypeInfo<ManifestEntry> {

        public TypeInfo() {
            super(ManifestEntry.class);
        }

        @Override
        public TypeSerializer<ManifestEntry> createSerializer(SerializerConfig config) {
            return new NoneCopyVersionedSerializerTypeSerializerProxy<>(
                    () ->
                            new SimpleVersionedSerializer<ManifestEntry>() {
                                private final ManifestEntrySerializer manifestEntrySerializer =
                                        new ManifestEntrySerializer();

                                @Override
                                public int getVersion() {
                                    return 0;
                                }

                                @Override
                                public byte[] serialize(ManifestEntry manifestEntry)
                                        throws IOException {
                                    return manifestEntrySerializer.serializeToBytes(manifestEntry);
                                }

                                @Override
                                public ManifestEntry deserialize(int i, byte[] bytes)
                                        throws IOException {
                                    return manifestEntrySerializer.deserializeFromBytes(bytes);
                                }
                            });
        }
    }
}
