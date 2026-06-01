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

import org.apache.paimon.append.dataevolution.DataEvolutionRowIdReassigner;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Action to reassign row IDs for data evolution tables. */
public class ReassignRowIdAction extends ActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(ReassignRowIdAction.class);

    private final Identifier identifier;
    private final Map<String, String> tableConf;
    private final List<Map<String, String>> partitionSpecs;

    public ReassignRowIdAction(
            Map<String, String> catalogConfig,
            String databaseName,
            String tableName,
            Map<String, String> tableConfig,
            List<Map<String, String>> partitionSpecs) {
        super(catalogConfig);
        this.identifier = Identifier.create(databaseName, tableName);
        this.tableConf = new HashMap<>(tableConfig);
        this.partitionSpecs = new ArrayList<>(partitionSpecs);
    }

    @Override
    public void build() throws Exception {
        Table table = catalog.getTable(identifier).copy(tableConf);
        checkArgument(
                table instanceof FileStoreTable,
                "Action '%s' only supports file store table, but table '%s' is %s.",
                ReassignRowIdActionFactory.IDENTIFIER,
                identifier.getFullName(),
                table.getClass().getName());

        DataStreamSource<Void> source =
                env.addSource(
                        new ReassignRowIdSource(
                                identifier.getFullName(), (FileStoreTable) table, partitionSpecs),
                        "Reassign Row ID : " + identifier.getFullName(),
                        BasicTypeInfo.VOID_TYPE_INFO);
        ((LegacySourceTransformation<?>) source.getTransformation())
                .setBoundedness(Boundedness.BOUNDED);
        source.setParallelism(1).forward().sinkTo(new DiscardingSink<>()).setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        env.execute("Reassign Row ID : " + identifier.getFullName());
    }

    public static @Nullable PartitionPredicate toPartitionPredicate(
            FileStoreTable table, List<Map<String, String>> partitionSpecs) {
        if (partitionSpecs.isEmpty()) {
            return null;
        }

        RowType partitionType = table.schema().logicalPartitionType();
        List<String> partitionKeys = table.partitionKeys();
        checkArgument(
                partitionType.getFieldCount() > 0,
                "Partition filter can only be used for partitioned table '%s'.",
                table.name());

        String defaultName = table.coreOptions().partitionDefaultName();
        for (Map<String, String> partitionSpec : partitionSpecs) {
            checkArgument(
                    partitionSpec.keySet().containsAll(partitionKeys)
                            && partitionKeys.containsAll(partitionSpec.keySet()),
                    "Partition filter for table '%s' must match partition keys %s, but was %s.",
                    table.name(),
                    partitionKeys,
                    partitionSpec);
        }

        return PartitionPredicate.fromMaps(partitionType, partitionSpecs, defaultName);
    }

    public static String formatResult(
            String tableName, DataEvolutionRowIdReassigner.Result result) {
        if (!result.reassigned) {
            String reason =
                    result.skipReason == null
                            ? "row IDs are already partition-contiguous"
                            : result.skipReason;
            return String.format(
                    "Skipped. Row IDs for table '%s' were not reassigned because %s:"
                            + " snapshot %d unchanged, nextRowId=%d.",
                    tableName, reason, result.previousSnapshotId, result.nextRowId);
        }
        return String.format(
                "Success. Reassigned row IDs for table '%s': snapshot %d -> %d,"
                        + " nextRowId %d -> %d, files=%d, rows=%d, indexFiles=%d.",
                tableName,
                result.previousSnapshotId,
                result.newSnapshotId,
                result.firstAssignedRowId,
                result.nextRowId,
                result.fileCount,
                result.rowCount,
                result.indexFileCount);
    }

    private static class ReassignRowIdSource implements SourceFunction<Void> {

        private static final long serialVersionUID = 1L;

        private final String tableName;
        private final FileStoreTable table;
        private final List<Map<String, String>> partitionSpecs;

        private ReassignRowIdSource(
                String tableName, FileStoreTable table, List<Map<String, String>> partitionSpecs) {
            this.tableName = tableName;
            this.table = table;
            this.partitionSpecs = partitionSpecs;
        }

        @Override
        public void run(SourceContext<Void> sourceContext) {
            DataEvolutionRowIdReassigner.Result result =
                    new DataEvolutionRowIdReassigner(
                                    table, toPartitionPredicate(table, partitionSpecs))
                            .reassign();
            String message = formatResult(tableName, result);
            LOG.info(message);
            System.out.println(message);
        }

        @Override
        public void cancel() {}
    }
}
