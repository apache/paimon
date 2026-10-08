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

package org.apache.paimon.migrate;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Imports existing files by recording their external paths in a single append commit. */
public class ExternalFileImporter {

    private ExternalFileImporter() {}

    public static long importFiles(Table table, String location, @Nullable String partition)
            throws Exception {
        checkArgument(table instanceof FileStoreTable, "import_files requires a FileStoreTable.");
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        checkArgument(
                table.primaryKeys().isEmpty() && fileStoreTable.coreOptions().bucket() == -1,
                "import_files only supports append tables with bucket = -1.");
        checkArgument(
                !fileStoreTable.coreOptions().rowTrackingEnabled()
                        && !fileStoreTable.coreOptions().dataEvolutionEnabled(),
                "import_files does not support row tracking or data-evolution tables.");

        Map<String, String> partitionSpec = parseCommaSeparatedKeyValues(partition);
        checkArgument(
                partitionSpec.keySet().equals(new HashSet<>(table.partitionKeys())),
                "Partition must specify exactly the table partition keys: %s.",
                table.partitionKeys());
        RowType partitionType = fileStoreTable.store().partitionType();
        BinaryRow partitionRow =
                FileMetaUtils.writePartitionValue(
                        partitionType,
                        table.partitionKeys().stream()
                                .map(partitionSpec::get)
                                .collect(Collectors.toList()),
                        partitionType.getFields().stream()
                                .map(f -> BinaryWriter.createValueSetter(f.type()))
                                .collect(Collectors.toList()),
                        fileStoreTable.coreOptions().partitionDefaultName());

        String fileFormat = fileStoreTable.coreOptions().fileFormatString();
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(location), "Location must not be empty.");
        Path directory = new Path(location);
        checkArgument(
                directory.toUri().getPath().startsWith("/"), "Location must be an absolute path.");
        FileIO fileIO = table.fileIO();
        checkArgument(fileIO.getFileStatus(directory).isDir(), "Location must be a directory.");

        Map<String, FileStatus> files = new LinkedHashMap<>();
        for (FileStatus status : fileIO.listStatus(directory)) {
            String name = status.getPath().getName();
            if (!status.isDir()
                    && !name.startsWith(".")
                    && !name.startsWith("_")
                    && name.endsWith("." + fileFormat)) {
                files.putIfAbsent(status.getPath().toString(), status);
            }
        }
        if (files.isEmpty()) {
            return 0;
        }

        Set<String> existingPaths = new HashSet<>();
        for (Split split : fileStoreTable.newScan().plan().splits()) {
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                file.externalPath().ifPresent(existingPaths::add);
            }
        }
        for (String externalPath : files.keySet()) {
            checkArgument(
                    !existingPaths.contains(externalPath),
                    "File has already been imported: %s.",
                    externalPath);
        }
        List<DataFileMeta> fileMetas = new ArrayList<>();
        SimpleStatsExtractor statsExtractor =
                FileMetaUtils.createSimpleStatsExtractor(table, fileFormat);
        for (FileStatus file : files.values()) {
            String externalPath = file.getPath().toString();
            // Manifest identity is independent of the source name, which can collide across dirs.
            DataFileMeta fileMeta =
                    FileMetaUtils.constructFileMeta(
                            "data-" + UUID.randomUUID() + "." + fileFormat,
                            file.getLen(),
                            file.getPath(),
                            statsExtractor,
                            fileIO,
                            table);
            checkArgument(
                    fileMeta.rowCount() >= 0,
                    "Cannot determine row count for file: %s.",
                    externalPath);
            fileMetas.add(fileMeta.newExternalPath(externalPath));
        }

        // Extract every file's metadata before committing, so a failed extraction adds no files.
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            FileMetaUtils.createCommitMessage(partitionRow, -1, fileMetas)));
        }
        return fileMetas.size();
    }
}
