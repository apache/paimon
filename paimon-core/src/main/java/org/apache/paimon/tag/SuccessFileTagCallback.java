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

package org.apache.paimon.tag;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.file.SuccessFile;
import org.apache.paimon.table.sink.TagCallback;

import java.io.IOException;

/** A {@link TagCallback} which create "{tagName}_SUCCESS" file. */
public class SuccessFileTagCallback implements TagCallback {
    private static final String SUCCESS_FILE_SUFFIX = "_SUCCESS";
    private static final String SUCCESS_FILE_DIRECTORY = "tag-success-file";

    private final FileIO fileIO;
    private final Path successFileDirectory;

    public SuccessFileTagCallback(FileIO fileIO, Path tagDirectory) {
        this.fileIO = fileIO;
        this.successFileDirectory = new Path(tagDirectory, SUCCESS_FILE_DIRECTORY);
        try {
            fileIO.checkOrMkdirs(successFileDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyCreation(String tagName) {
        Path tagSuccessPath = tagSuccessFilePath(tagName);
        long currentTime = System.currentTimeMillis();
        SuccessFile successFile = new SuccessFile(currentTime, currentTime);

        try {
            if (fileIO.exists(tagSuccessPath)) {
                successFile =
                        SuccessFile.fromPath(fileIO, tagSuccessPath)
                                .updateModificationTime(currentTime);
            }
            fileIO.overwriteFileUtf8(tagSuccessPath, successFile.toJson());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void notifyDeletion(String tagName) {
        Path tagSuccessPath = tagSuccessFilePath(tagName);
        fileIO.deleteQuietly(tagSuccessPath);
    }

    @VisibleForTesting
    public Path tagSuccessFilePath(String tagName) {
        return new Path(successFileDirectory, tagName + SUCCESS_FILE_SUFFIX);
    }

    @Override
    public void close() throws Exception {}
}
