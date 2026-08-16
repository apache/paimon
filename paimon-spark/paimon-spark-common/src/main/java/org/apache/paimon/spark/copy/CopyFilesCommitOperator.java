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

import org.apache.spark.api.java.JavaRDD;
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
            JavaRDD<CopyFileInfo> dataCopyFileInfoRdd,
            JavaRDD<CopyFileInfo> indexCopyFileInfoRdd)
            throws Exception {
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

        // deserialize data file meta
        Map<Tuple2<BinaryRow, Integer>, DataFileInfo> dataFileMetaMap =
                deserializeDataFileMeta(dataCopyFileInfoRdd);

        // deserialize index file meta
        Map<Tuple2<BinaryRow, Integer>, IndexFileInfo> indexFileMetaMap =
                deserializeIndexFileMeta(indexCopyFileInfoRdd);

        // construct commit messages
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Map.Entry<Tuple2<BinaryRow, Integer>, DataFileInfo> entry :
                dataFileMetaMap.entrySet()) {
            Tuple2<BinaryRow, Integer> partitionAndBucket = entry.getKey();
            DataFileInfo dataFileInfo = entry.getValue();
            List<DataFileMeta> dataFileMetas = dataFileInfo.dataFileMetas();
            List<IndexFileMeta> indexFileMetas = new ArrayList<>();
            if (indexFileMetaMap.containsKey(partitionAndBucket)) {
                IndexFileInfo indexFileInfo = indexFileMetaMap.get(partitionAndBucket);
                indexFileMetas.addAll(indexFileInfo.indexFileMetas());
            }
            commitMessages.add(
                    FileMetaUtils.createCommitMessage(
                            partitionAndBucket._1,
                            partitionAndBucket._2,
                            dataFileInfo.totalBuckets(),
                            dataFileMetas,
                            indexFileMetas));
        }
        try (BatchTableCommit commit =
                targetTable.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }
    }

    private Map<Tuple2<BinaryRow, Integer>, DataFileInfo> deserializeDataFileMeta(
            JavaRDD<CopyFileInfo> copyFileInfoRdd) throws IOException {
        Map<Tuple2<BinaryRow, Integer>, DataFileInfo> result = new HashMap<>();
        if (copyFileInfoRdd == null) {
            return result;
        }
        List<CopyFileInfo> copyFileInfos = copyFileInfoRdd.collect();
        for (CopyFileInfo copyDataFileInfo : copyFileInfos) {
            BinaryRow partition =
                    SerializationUtils.deserializeBinaryRow(copyDataFileInfo.partition());
            int bucket = copyDataFileInfo.bucket();
            DataFileInfo dataFileInfo =
                    result.computeIfAbsent(
                            new Tuple2<>(partition, bucket),
                            k ->
                                    new DataFileInfo(
                                            partition,
                                            bucket,
                                            copyDataFileInfo.totalBuckets(),
                                            new ArrayList<>()));
            dataFileInfo
                    .dataFileMetas()
                    .add(dataFileSerializer.deserializeFromBytes(copyDataFileInfo.dataFileMeta()));
        }
        return result;
    }

    private Map<Tuple2<BinaryRow, Integer>, IndexFileInfo> deserializeIndexFileMeta(
            JavaRDD<CopyFileInfo> copyFileInfoRdd) throws IOException {
        Map<Tuple2<BinaryRow, Integer>, IndexFileInfo> result = new HashMap<>();
        if (copyFileInfoRdd == null) {
            return result;
        }
        List<CopyFileInfo> copyFileInfos = copyFileInfoRdd.collect();
        for (CopyFileInfo copyIndexFileInfo : copyFileInfos) {
            BinaryRow partition =
                    SerializationUtils.deserializeBinaryRow(copyIndexFileInfo.partition());
            int bucket = copyIndexFileInfo.bucket();
            IndexFileInfo indexFileInfo =
                    result.computeIfAbsent(
                            new Tuple2<>(partition, bucket),
                            k -> new IndexFileInfo(partition, bucket, new ArrayList<>()));
            indexFileInfo
                    .indexFileMetas()
                    .add(
                            indexFileSerializer.deserializeFromBytes(
                                    copyIndexFileInfo.dataFileMeta()));
        }
        return result;
    }
}
