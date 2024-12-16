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

package org.apache.paimon.table.object;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Util class for refreshing object table. */
public class ObjectRefresh {

    public static long refresh(ObjectTable table) throws Exception {
        String location = table.objectLocation();

        // 1. collect all files for object table
        List<FileStatus> fileCollector = new ArrayList<>();
        listAllFiles(table.objectFileIO(), new Path(location), fileCollector);

        // 2. write to underlying table
        BatchWriteBuilder writeBuilder =
                table.underlyingTable().newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (FileStatus file : fileCollector) {
                write.write(toRow(file));
            }
            commit.commit(write.prepareCommit());
        }

        return fileCollector.size();
    }

    private static void listAllFiles(FileIO fileIO, Path directory, List<FileStatus> fileCollector)
            throws IOException {
        FileStatus[] files = fileIO.listStatus(directory);
        if (files == null) {
            return;
        }

        for (FileStatus file : files) {
            if (file.isDir()) {
                listAllFiles(fileIO, file.getPath(), fileCollector);
            } else {
                fileCollector.add(file);
            }
        }
    }

    private static InternalRow toRow(FileStatus file) {
        return toRow(
                file.getPath().toString(),
                file.getPath().getParent().toString(),
                file.getPath().getName(),
                file.getLen(),
                Timestamp.fromEpochMillis(file.getModificationTime()),
                Timestamp.fromEpochMillis(file.getAccessTime()),
                file.getOwner(),
                null,
                null,
                null,
                null,
                null,
                new GenericMap(Collections.emptyMap()));
    }

    public static GenericRow toRow(Object... values) {
        GenericRow row = new GenericRow(values.length);

        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            if (value instanceof String) {
                value = BinaryString.fromString((String) value);
            }
            row.setField(i, value);
        }

        return row;
    }
}
