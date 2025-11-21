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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Copy data files from source table to target table. */
public class CopyDataFilesOperator extends CopyFilesOperator {

    public CopyDataFilesOperator(SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
    }

    public JavaRDD<CopyFileInfo> execute(
            Identifier sourceIdentifier, Identifier targetIdentifier, List<CopyFileInfo> dataFiles)
            throws Exception {
        if (CollectionUtils.isEmpty(dataFiles)) {
            return null;
        }
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);
        int readParallelism = SparkProcedureUtils.readParallelism(dataFiles, spark);
        JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<CopyFileInfo> copyFileInfoRdd =
                context.parallelize(dataFiles, readParallelism)
                        .mapPartitions(new DataFileProcesser(sourceTable, targetTable));
        return copyFileInfoRdd;
    }

    /** Copy data files. */
    public static class DataFileProcesser
            implements FlatMapFunction<Iterator<CopyFileInfo>, CopyFileInfo> {

        private final FileStoreTable sourceTable;
        private final FileStoreTable targetTable;

        public DataFileProcesser(FileStoreTable sourceTable, FileStoreTable targetTable) {
            this.sourceTable = sourceTable;
            this.targetTable = targetTable;
        }

        @Override
        public Iterator<CopyFileInfo> call(Iterator<CopyFileInfo> dataFileIterator)
                throws Exception {
            List<CopyFileInfo> result = new ArrayList<>();
            while (dataFileIterator.hasNext()) {
                CopyFileInfo file = dataFileIterator.next();
                Path sourcePath = new Path(file.sourceFilePath());
                Path targetPath = new Path(file.targetFilePath());
                CopyFilesUtil.copyFiles(
                        sourceTable.fileIO(), targetTable.fileIO(), sourcePath, targetPath, false);
                result.add(file);
            }
            return result.iterator();
        }
    }
}
