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

import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SerializationUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** List index files. */
public class ListIndexFilesOperator extends CopyFilesOperator {

    public ListIndexFilesOperator(
            SparkSession spark, Catalog sourceCatalog, Catalog targetCatalog) {
        super(spark, sourceCatalog, targetCatalog);
    }

    public JavaRDD<CopyDataFileInfo> execute(
            Identifier sourceIdentifier,
            List<CopyFileInfo> manifestFiles,
            @Nullable PartitionPredicate partitionPredicate)
            throws Exception {
        if (CollectionUtils.isEmpty(manifestFiles)) {
            return null;
        }
        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        int readParallelism = SparkProcedureUtils.readParallelism(manifestFiles, spark);
        JavaRDD<CopyDataFileInfo> dataFilesJavaRdd =
                javaSparkContext
                        .parallelize(manifestFiles, readParallelism)
                        .mapPartitions(
                                new IndexManifestFileProcesser(sourceTable, partitionPredicate));
        return dataFilesJavaRdd;
    }

    /** Process manifest files. */
    public static class IndexManifestFileProcesser
            implements FlatMapFunction<Iterator<CopyFileInfo>, CopyDataFileInfo> {

        private final FileStoreTable sourceTable;
        @Nullable private final PartitionPredicate partitionPredicate;

        public IndexManifestFileProcesser(
                FileStoreTable sourceTable, @Nullable PartitionPredicate partitionPredicate) {
            this.sourceTable = sourceTable;
            this.partitionPredicate = partitionPredicate;
        }

        @Override
        public Iterator<CopyDataFileInfo> call(Iterator<CopyFileInfo> manifestFileIterator)
                throws Exception {
            List<CopyDataFileInfo> indexFiles = new ArrayList<>();
            FileStore<?> sourceStore = sourceTable.store();
            IndexFileHandler indexFileHandler = sourceStore.newIndexFileHandler();
            IndexFileMetaSerializer indexFileSerializer = new IndexFileMetaSerializer();
            while (manifestFileIterator.hasNext()) {
                CopyFileInfo manifestFileCopyFileInfo = manifestFileIterator.next();
                Path sourcePath = new Path(manifestFileCopyFileInfo.sourceFilePath());
                List<IndexManifestEntry> indexManifestEntries =
                        indexFileHandler.readManifestWithIOException(sourcePath.getName());
                for (IndexManifestEntry manifestEntry : indexManifestEntries) {
                    if (partitionPredicate == null
                            || partitionPredicate.test(manifestEntry.partition())) {
                        CopyDataFileInfo indexFile =
                                pickIndexFiles(
                                        manifestEntry, indexFileHandler, indexFileSerializer);
                        indexFiles.add(indexFile);
                    }
                }
            }
            return indexFiles.iterator();
        }

        private CopyDataFileInfo pickIndexFiles(
                IndexManifestEntry indexManifestEntry,
                IndexFileHandler indexFileHandler,
                IndexFileMetaSerializer indexFileSerializer)
                throws IOException {
            Path indexFilePath = indexFileHandler.filePath(indexManifestEntry);
            Path relativePath =
                    CopyFilesUtil.getPathExcludeTableRoot(indexFilePath, sourceTable.location());
            return new CopyDataFileInfo(
                    indexFilePath.toString(),
                    relativePath.toString(),
                    SerializationUtils.serializeBinaryRow(indexManifestEntry.partition()),
                    indexFileSerializer.serializeToBytes(indexManifestEntry.indexFile()));
        }
    }
}
