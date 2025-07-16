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

package org.apache.paimon.flink.clone.files;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.clone.HivePartitionFiles;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Clone File (table, partition) with necessary information. */
public class CloneFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Identifier identifier;
    private final byte[] partition;
    private final Path path;
    private final long fileSize;
    private final String format;

    public CloneFileInfo(
            Identifier identifier, BinaryRow partition, Path path, long fileSize, String format) {
        this.identifier = identifier;
        this.partition = serializeBinaryRow(partition);
        this.path = path;
        this.fileSize = fileSize;
        this.format = format;
    }

    public Identifier identifier() {
        return identifier;
    }

    public BinaryRow partition() {
        return deserializeBinaryRow(partition);
    }

    public Path path() {
        return path;
    }

    public long fileSize() {
        return fileSize;
    }

    public String format() {
        return format;
    }

    public static List<CloneFileInfo> fromHive(Identifier identifier, HivePartitionFiles files) {
        List<CloneFileInfo> result = new ArrayList<>();
        for (int i = 0; i < files.paths().size(); i++) {
            Path path = files.paths().get(i);
            long fileSize = files.fileSizes().get(i);
            result.add(
                    new CloneFileInfo(
                            identifier, files.partition(), path, fileSize, files.format()));
        }
        return result;
    }
}
