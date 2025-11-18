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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Commit operator for copy files. */
public class CopyFilesCommitOperator extends CopyFilesOperator {

    private DataFileMetaSerializer dataFileSerializer;

    public CopyFilesCommitOperator(
            SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    public void execute(
            Identifier targetIdentifier, JavaPairRDD<byte[], byte[]> dataFileMetaPairRdd)
            throws Exception {
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);
        Map<byte[], Iterable<byte[]>> dataFileMetaMap =
                dataFileMetaPairRdd.groupByKey().collectAsMap();
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Map.Entry<byte[], Iterable<byte[]>> entry : dataFileMetaMap.entrySet()) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(entry.getKey());
            List<DataFileMeta> dataFileMetas = new ArrayList<>();
            for (byte[] bytes : entry.getValue()) {
                dataFileMetas.add(dataFileSerializer.deserializeFromBytes(bytes));
            }
            commitMessages.add(
                    FileMetaUtils.createCommitMessage(
                            partition, targetTable.coreOptions().bucket(), dataFileMetas));
        }
        try (BatchTableCommit commit =
                targetTable.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(commitMessages);
        }
    }
}
