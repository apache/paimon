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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.Optional;

/** Utils for copy files. */
public class CopyFilesUtil {

    public static void copyFiles(
            FileIO sourceFileIO,
            FileIO targetFileIO,
            Path sourcePath,
            Path targetPath,
            boolean overwrite)
            throws IOException {
        try (SeekableInputStream is = sourceFileIO.newInputStream(sourcePath);
                PositionOutputStream os = targetFileIO.newOutputStream(targetPath, overwrite)) {
            IOUtils.copy(is, os);
        }
    }

    public static DataFileMeta toNewDataFileMeta(
            DataFileMeta oldFileMeta, String newFileName, long newSchemaId) {
        String newExternalPath =
                externalPathDir(oldFileMeta.externalPath().orElse(null))
                        .map(dir -> dir + "/" + newFileName)
                        .orElse(null);
        return new PojoDataFileMeta(
                newFileName,
                oldFileMeta.fileSize(),
                oldFileMeta.rowCount(),
                oldFileMeta.minKey(),
                oldFileMeta.maxKey(),
                oldFileMeta.keyStats(),
                oldFileMeta.valueStats(),
                oldFileMeta.minSequenceNumber(),
                oldFileMeta.maxSequenceNumber(),
                newSchemaId,
                oldFileMeta.level(),
                oldFileMeta.extraFiles(),
                oldFileMeta.creationTime(),
                oldFileMeta.deleteRowCount().orElse(null),
                oldFileMeta.embeddedIndex(),
                oldFileMeta.fileSource().orElse(null),
                oldFileMeta.valueStatsCols(),
                newExternalPath,
                oldFileMeta.firstRowId(),
                oldFileMeta.writeCols());
    }

    public static IndexFileMeta toNewIndexFileMeta(IndexFileMeta oldFileMeta, String newFileName) {
        String newExternalPath =
                externalPathDir(oldFileMeta.externalPath())
                        .map(dir -> dir + "/" + newFileName)
                        .orElse(null);
        return new IndexFileMeta(
                oldFileMeta.indexType(),
                newFileName,
                oldFileMeta.fileSize(),
                oldFileMeta.rowCount(),
                oldFileMeta.dvRanges(),
                newExternalPath);
    }

    public static Optional<String> externalPathDir(String externalPath) {
        return Optional.ofNullable(externalPath)
                .map(Path::new)
                .map(p -> p.getParent().toUri().toString());
    }
}
