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
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.schema.TableSchema;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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
        return toNewDataFileMeta(oldFileMeta, newFileName, newSchemaId, null);
    }

    public static DataFileMeta toNewDataFileMeta(
            DataFileMeta oldFileMeta,
            String newFileName,
            long newSchemaId,
            @Nullable TableSchema sourceFileSchema) {
        String newExternalPath =
                externalPathDir(oldFileMeta.externalPath().orElse(null))
                        .map(dir -> dir + "/" + newFileName)
                        .orElse(null);
        List<String> writeCols = oldFileMeta.writeCols();
        if (sourceFileSchema != null && writeCols == null) {
            // A null value is interpreted using the schema id. Materialize its source meaning
            // before rebinding the file to the target schema, whether it denotes the full schema
            // (legacy) or all non-dedicated columns (compact metadata).
            writeCols = sourceFileSchema.dataFileSchema(null).fieldNames();
        }
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
                writeCols,
                // Column sequence numbers are positional and cannot be safely reused after
                // changing the schema id. A null value makes readers fall back conservatively.
                null);
    }

    public static IndexFileMeta toNewIndexFileMeta(
            IndexFileMeta oldFileMeta, String newFileName, @Nullable Long newSchemaId) {
        return toNewIndexFileMeta(
                oldFileMeta, newFileName, newSchemaId, oldFileMeta.globalIndexMeta());
    }

    public static IndexFileMeta toNewPrimaryKeyIndexFileMeta(
            IndexFileMeta oldFileMeta,
            String newFileName,
            Map<String, String> dataFileNameMapping) {
        PrimaryKeyIndexSourceMeta oldSourceMeta =
                PrimaryKeyIndexSourceMeta.fromIndexFile(oldFileMeta);
        ArrayList<PrimaryKeyIndexSourceFile> newSourceFiles = new ArrayList<>();
        for (PrimaryKeyIndexSourceFile sourceFile : oldSourceMeta.sourceFiles()) {
            String newSourceFileName = dataFileNameMapping.get(sourceFile.fileName());
            checkArgument(
                    newSourceFileName != null,
                    "Cannot find copied data file for primary-key index source %s.",
                    sourceFile.fileName());
            newSourceFiles.add(
                    new PrimaryKeyIndexSourceFile(newSourceFileName, sourceFile.rowCount()));
        }

        GlobalIndexMeta oldGlobalIndexMeta = oldFileMeta.globalIndexMeta();
        GlobalIndexMeta newGlobalIndexMeta =
                new GlobalIndexMeta(
                        oldGlobalIndexMeta.rowRangeStart(),
                        oldGlobalIndexMeta.rowRangeEnd(),
                        oldGlobalIndexMeta.indexFieldId(),
                        oldGlobalIndexMeta.extraFieldIds(),
                        oldGlobalIndexMeta.indexMeta(),
                        new PrimaryKeyIndexSourceMeta(oldSourceMeta.dataLevel(), newSourceFiles)
                                .serialize());
        return toNewIndexFileMeta(
                oldFileMeta, newFileName, oldFileMeta.schemaId(), newGlobalIndexMeta);
    }

    private static IndexFileMeta toNewIndexFileMeta(
            IndexFileMeta oldFileMeta,
            String newFileName,
            @Nullable Long newSchemaId,
            @Nullable GlobalIndexMeta newGlobalIndexMeta) {
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
                newExternalPath,
                newGlobalIndexMeta,
                newSchemaId);
    }

    public static Optional<String> externalPathDir(String externalPath) {
        return Optional.ofNullable(externalPath).map(Path::new).map(p -> p.getParent().toString());
    }
}
