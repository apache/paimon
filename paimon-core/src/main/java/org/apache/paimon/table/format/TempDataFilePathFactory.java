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
import org.apache.paimon.io.DataFilePathFactory;

import java.util.List;

/**
 * A DataFilePathFactory wrapper that creates temporary file paths for HDFS atomic writes. It
 * creates files in the temp directory and tracks them for later rename operations.
 */
public class TempDataFilePathFactory extends DataFilePathFactory {

    private final DataFilePathFactory originalFactory;
    private final Path tempDirectory;
    private final BinaryRow partition;
    private final List<FormatTableFileInfo> tempFileList;

    public TempDataFilePathFactory(
            DataFilePathFactory originalFactory,
            Path tempDirectory,
            BinaryRow partition,
            List<FormatTableFileInfo> tempFileList,
            String formatIdentifier,
            String fileCompression) {
        // Initialize the parent class with temp directory
        super(
                tempDirectory, // parent directory - use temp directory
                formatIdentifier, // formatIdentifier - passed from external
                "data-", // dataFilePrefix
                "changelog-", // changelogFilePrefix
                false, // fileSuffixIncludeCompression
                fileCompression, // fileCompression
                null); // externalPathProvider

        this.originalFactory = originalFactory;
        this.tempDirectory = tempDirectory;
        this.partition = partition;
        this.tempFileList = tempFileList;
    }

    @Override
    public Path newPath() {
        // Generate temp path using parent class
        Path tempPath = super.newPath();

        // Get the target path from original factory
        Path targetPath = originalFactory.newPath();

        // Record the mapping for later rename operation
        FormatTableFileInfo fileInfo = new FormatTableFileInfo(partition, tempPath, targetPath);
        synchronized (tempFileList) {
            tempFileList.add(fileInfo);
        }

        return tempPath;
    }

    @Override
    public Path newChangelogPath() {
        // Generate temp changelog path using parent class
        Path tempPath = super.newChangelogPath();

        // Get the target path from original factory
        Path targetPath = originalFactory.newChangelogPath();

        // Record the mapping for later rename operation
        FormatTableFileInfo fileInfo = new FormatTableFileInfo(partition, tempPath, targetPath);
        synchronized (tempFileList) {
            tempFileList.add(fileInfo);
        }

        return tempPath;
    }
}
