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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;

/**
 * Information about a file written by FormatTableFileWrite, including both temporary and target
 * paths for HDFS atomic writes.
 */
public class FormatTableFileInfo {

    private final BinaryRow partition;
    private final Path tempPath;
    private final Path targetPath;

    public FormatTableFileInfo(BinaryRow partition, Path tempPath, Path targetPath) {
        this.partition = partition;
        this.tempPath = tempPath;
        this.targetPath = targetPath;
    }

    public BinaryRow getPartition() {
        return partition;
    }

    public Path getTempPath() {
        return tempPath;
    }

    public Path getTargetPath() {
        return targetPath;
    }

    @Override
    public String toString() {
        return String.format(
                "FormatTableFileInfo{partition=%s, tempPath=%s, targetPath=%s}",
                partition, tempPath, targetPath);
    }
}
