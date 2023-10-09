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

package org.apache.paimon.flink.query;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** Operator for query executor. */
public class QueryExecutorOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow> {

    private static final long serialVersionUID = 1L;

    private final Catalog.Loader catalogLoader;

    private transient Catalog catalog;
    private transient Map<Identifier, TableQuery> queryMap;

    public QueryExecutorOperator(Catalog.Loader catalogLoader) {
        this.catalogLoader = catalogLoader;
    }

    public static RowType outputType() {
        return RowType.of(
                DataTypes.STRING(),
                DataTypes.STRING(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.STRING(),
                DataTypes.INT());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.catalog = catalogLoader.load();
        this.queryMap = new HashMap<>();
    }

    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow row = streamRecord.getValue();
        String database = row.getString(0).toString();
        String tableName = row.getString(1).toString();
        long snapshotId = row.getLong(2);
        BinaryRow partition = deserializeBinaryRow(row.getBinary(3));
        int bucket = row.getInt(4);
        DataFileMetaSerializer fileMetaSerializer = new DataFileMetaSerializer();
        List<DataFileMeta> beforeFiles = fileMetaSerializer.deserializeList(row.getBinary(5));
        List<DataFileMeta> dataFiles = fileMetaSerializer.deserializeList(row.getBinary(6));

        Identifier identifier = new Identifier(database, tableName);
        TableQuery query = queryMap.get(identifier);
        if (query == null) {
            Table table = catalog.getTable(identifier);
            query = ((FileStoreTable) table).newQuery();
            queryMap.put(identifier, query);
        }

        query.refreshFiles(snapshotId, partition, bucket, beforeFiles, dataFiles);

        this.output.collect(
                new StreamRecord<>(
                        GenericRow.of(
                                row.getString(0),
                                row.getString(1),
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                getRuntimeContext().getIndexOfThisSubtask(),
                                "hostname",
                                0)));
    }

    @Nullable
    public InternalRow lookup(
            Identifier identifier, BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        TableQuery query = queryMap.get(identifier);
        if (query == null) {
            return null;
        }

        return query.lookup(partition, bucket, key);
    }

    @Override
    public void close() throws Exception {
        super.close();
        queryMap.values().forEach(IOUtils::closeQuietly);
        queryMap.clear();
    }
}
