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

package org.apache.paimon.spark.copy;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/** Commit operator for copy files. */
public class CopyFilesCommitOperator extends CopyFilesOperator {

    private DataFileMetaSerializer dataFileSerializer;

    private IndexFileMetaSerializer indexFileSerializer;

    public CopyFilesCommitOperator(
            SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.indexFileSerializer = new IndexFileMetaSerializer();
    }

    public void execute(
            Identifier targetIdentifier,
            JavaPairRDD<byte[], byte[]> dataFileMetaPairRdd,
            JavaPairRDD<byte[], byte[]> indexFileMetaPairRdd)
            throws Exception {
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

        // deserialize data file meta
        Map<BinaryRow, List<DataFileMeta>> dataFileMetaMap =
                deserializeDataFileMeta(dataFileMetaPairRdd);

        // deserialize index file meta
        Map<BinaryRow, List<IndexFileMeta>> indexFileMetaMap =
                deserializeIndexFileMeta(indexFileMetaPairRdd);

        // construct commit messages
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (BinaryRow partition : dataFileMetaMap.keySet()) {
            List<DataFileMeta> dataFileMetas = dataFileMetaMap.get(partition);
            List<IndexFileMeta> indexFileMetas =
                    indexFileMetaMap.getOrDefault(partition, new ArrayList<>());
            commitMessages.add(
                    FileMetaUtils.createCommitMessage(
                            partition,
                            targetTable.coreOptions().bucket(),
                            dataFileMetas,
                            indexFileMetas));
        }
        try (BatchTableCommit commit =
                targetTable.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private Map<BinaryRow, List<DataFileMeta>> deserializeDataFileMeta(
            JavaPairRDD<byte[], byte[]> dataFileMetaPairRdd) throws IOException {
        Map<BinaryRow, List<DataFileMeta>> result = new HashMap<>();
        if (dataFileMetaPairRdd == null) {
            return result;
        }
        List<Tuple2<byte[], byte[]>> dataFileMetaByteList = dataFileMetaPairRdd.collect();
        for (Tuple2<byte[], byte[]> tuple : dataFileMetaByteList) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(tuple._1());
            List<DataFileMeta> fileMetas =
                    result.computeIfAbsent(partition, k -> new ArrayList<>());
            fileMetas.add(dataFileSerializer.deserializeFromBytes(tuple._2()));
        }
        return result;
    }

    private Map<BinaryRow, List<IndexFileMeta>> deserializeIndexFileMeta(
            JavaPairRDD<byte[], byte[]> indexFileMetaPairRdd) throws IOException {
        Map<BinaryRow, List<IndexFileMeta>> result = new HashMap<>();
        if (indexFileMetaPairRdd == null) {
            return result;
        }
        List<Tuple2<byte[], byte[]>> indexFileMetaByteList = indexFileMetaPairRdd.collect();
        for (Tuple2<byte[], byte[]> tuple : indexFileMetaByteList) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(tuple._1());
            List<IndexFileMeta> fileMetas =
                    result.computeIfAbsent(partition, k -> new ArrayList<>());
            fileMetas.add(indexFileSerializer.deserializeFromBytes(tuple._2()));
        }
        return result;
    }
}
