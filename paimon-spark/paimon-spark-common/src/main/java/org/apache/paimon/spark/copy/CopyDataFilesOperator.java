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
import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

/** Copy data files from source table to target table. */
public class CopyDataFilesOperator extends CopyFilesOperator {

    public CopyDataFilesOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
    }

    public JavaPairRDD<byte[], byte[]> execute(
            Identifier sourceIdentifier,
            Identifier targetIdentifier,
            JavaRDD<CopyDataFileInfo> dataFiles)
            throws Exception {
        if (dataFiles == null) {
            return null;
        }
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);
        int readParallelism = SparkProcedureUtils.readParallelism(spark);
        JavaPairRDD<byte[], byte[]> partitionedDataFileMetas =
                dataFiles
                        .repartition(readParallelism)
                        .mapPartitionsToPair(new DataFileProcesser(sourceTable, targetTable));
        return partitionedDataFileMetas;
    }

    /** Copy data files. */
    public static class DataFileProcesser
            implements PairFlatMapFunction<Iterator<CopyDataFileInfo>, byte[], byte[]> {

        private final FileStoreTable sourceTable;
        private final FileStoreTable targetTable;

        public DataFileProcesser(FileStoreTable sourceTable, FileStoreTable targetTable) {
            this.sourceTable = sourceTable;
            this.targetTable = targetTable;
        }

        @Override
        public Iterator<Tuple2<byte[], byte[]>> call(Iterator<CopyDataFileInfo> dataFileIterator)
                throws Exception {
            List<Tuple2<byte[], byte[]>> result = new ArrayList<>();
            Path targetTableRootPath = targetTable.location();
            while (dataFileIterator.hasNext()) {
                CopyDataFileInfo dataFile = dataFileIterator.next();
                String filePathExcludeTableRoot = dataFile.filePathExcludeTableRoot();
                Path sourcePath = new Path(dataFile.sourceFilePath());
                Path targetPath = new Path(targetTableRootPath + filePathExcludeTableRoot);
                CopyFilesUtil.copyFiles(
                        sourceTable.fileIO(), targetTable.fileIO(), sourcePath, targetPath, true);
                result.add(new Tuple2<>(dataFile.partition(), dataFile.dataFileMeta()));
            }
            return result.iterator();
        }
    }
}
